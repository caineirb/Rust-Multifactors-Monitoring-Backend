use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use log::{info, warn, error};
use chrono::Utc;
use rusqlite::{
    params, 
    Connection, 
    Transaction, 
    Statement,
    Row, 
    Result as SqlResult,
    Error as SqlError,
    ErrorCode as SqlErrorCode,
};

pub struct Token {
    pub access_token: String,
    pub expires_at: String,
    pub created_at: String,
}

pub struct Site {
    pub name: String,
    pub groupid: i32,
    pub latitude: Option<f32>, // REAL is only 8-bit float in SQLite
    pub longitude: Option<f32>,
    pub last_synced: String,
    pub created_at: String,
    pub updated_at: Option<String>,
}

pub struct Device {
    pub id: i32,
    pub name: String,
    pub serial_number: Option<String>,
    pub mac: Option<String>,
    pub device_type: Option<String>,
    pub device_status: Option<String>,
    pub status_details: Option<String>,
    pub local_ip: Option<String>,
    pub public_ip: Option<String>,
    pub last_online_at: Option<String>,
    pub device_created_at: Option<String>,
    pub group_id: i32,
    pub last_synced: String,
    pub created_at: String,
    pub updated_at: String,
}

pub struct DatabaseManager {
    pub db_path: String,
}

impl DatabaseManager {
    pub fn new(db_path: Option<&str>) -> Self {
        let resolved_path: PathBuf = if let Some(path) = db_path {
            // User provided a path
            PathBuf::from(path)
        } else {
            // Determine environment (development or packaged)
            let data_dir: PathBuf;

            // Try to detect if running as compiled binary (similar to PyInstaller frozen)
            if let Ok(exe_path) = env::current_exe() {
                let exe_dir: PathBuf = exe_path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();

                // If executable is in a frozen-style bundle (you can customize detection)
                if exe_dir.join("data").exists() {
                    // PyInstaller-style (next to executable)
                    data_dir = exe_dir.join("data");
                } else {
                    // Development environment — go two levels up and add `data/`
                    if let Ok(current_dir) = env::current_dir() {
                        data_dir = current_dir.join("data");
                    } else {
                        data_dir = PathBuf::from("data");
                    }
                }
            } else {
                data_dir = PathBuf::from("data");
            }

            // Ensure data directory exists
            if let Err(e) = fs::create_dir_all(&data_dir) {
                warn!("Failed to create data directory {:?}: {}", data_dir, e);
            }

            // Final database path
            data_dir.join("multifactors.db")
        };

        let resolved_str = resolved_path.to_string_lossy().to_string();
        info!("Database path: {}", resolved_str);

        DatabaseManager {
            db_path: resolved_str,
        }
    }

    pub fn get_connection(&self) -> SqlResult<Connection> {
        match Connection::open(&self.db_path) {
            Ok(conn) => Ok(conn),
            Err(e) => {
                warn!("Failed to connect to database at {}: {}", self.db_path, e);
                Err(e)
            }
        }
    }

    pub fn initialize_database(&self) -> SqlResult<()> {
        let mut conn: Connection = self.get_connection()?;

        // Create tables transaction
        let trans: Transaction<'_> = conn.transaction()?;
        // Enable WAL mode for better concurrency
        trans.execute_batch(
        "PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA cache_size=1000;
            PRAGMA temp_store=memory;"
        )?;
        
        // table for access tokens
        const CREATE_ACCESS_TOKENS: &str = "
            CREATE TABLE IF NOT EXISTS access_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                access_token TEXT NOT NULL,
                expires_at DATETIME NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        ";
        // Table for api logs
        const CREATE_API_LOGS: &str = "
            CREATE TABLE IF NOT EXISTS api_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint TEXT NOT NULL,
                method TEXT NOT NULL,
                status_code INTEGER,
                response_message TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        ";
        // Table for site data
        const CREATE_SITES: &str = "
            CREATE TABLE IF NOT EXISTS sites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                groupid INTEGER NOT NULL UNIQUE,
                latitude REAL,
                longitude REAL,
                last_synced DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        ";
        // Table for devices data
        const CREATE_DEVICES: &str = "
            CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                serial_number TEXT,
                mac TEXT,
                device_type TEXT,
                device_status TEXT,
                status_details TEXT,
                local_ip TEXT,
                public_ip TEXT,
                last_online_at DATETIME,
                device_created_at DATETIME,
                group_id INTEGER NOT NULL,
                last_synced DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES sites (groupid)
            );
        ";
        // Execute all table creation statements in a single batch
        trans.execute_batch(&format!(
            "{}{}{}{}",
            CREATE_ACCESS_TOKENS, CREATE_API_LOGS, CREATE_SITES, CREATE_DEVICES
        ))?;
        trans.commit()?;

        let trans: Transaction<'_> = conn.transaction()?;

        // Check if devices table needs migration (remove UNIQUE constraint from serial_number)
        let needs_migration: bool = trans.query_row(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='devices'",
            [],
            |row: &Row<'_>| row.get::<_, String>(0),
        )
        .ok()
        .map(|sql: String| sql.contains("serial_number TEXT NOT NULL UNIQUE"))
        .unwrap_or(false);

        if needs_migration {
            info!("Migrating devices table to remove UNIQUE constraint from serial_number");
            
            match self.migrate_devices_table(&trans, CREATE_DEVICES) {
                Ok(_) => {
                    trans.commit()?;
                    info!("Migration completed successfully.");
                }
                Err(e) => {
                    trans.rollback()?;
                    return Err(e);
                }
            }
        }
        info!("Database initialized successfully.");
        match Ok(()) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Database initialization failed: {}", e);
                Err(e)
            }
        }
    }

    fn migrate_devices_table(&self, trans: &Transaction<'_>, create_table_sql: &str) -> SqlResult<()> {
        // Backup existing devices - exclude id column for reinsertion
        let mut stmt: Statement<'_> = trans.prepare(
            "SELECT name, serial_number, mac, device_type, device_status, status_details, 
                local_ip, public_ip, last_online_at, device_created_at, group_id, 
                last_synced, created_at, updated_at 
                FROM devices"
        )?;

        let exisiting_devices: Vec<_> = stmt.query_map([], |row: &Row<'_>| {
            Ok((
                row.get::<_, String>(0)?,           // name
                row.get::<_, Option<String>>(1)?,   // serial_number
                row.get::<_, Option<String>>(2)?,   // mac
                row.get::<_, Option<String>>(3)?,   // device_type
                row.get::<_, Option<String>>(4)?,   // device_status
                row.get::<_, Option<String>>(5)?,   // status_details
                row.get::<_, Option<String>>(6)?,   // local_ip
                row.get::<_, Option<String>>(7)?,   // public_ip
                row.get::<_, Option<String>>(8)?,   // last_online_at
                row.get::<_, Option<String>>(9)?,   // device_created_at
                row.get::<_, i32>(10)?,             // group_id
                row.get::<_, String>(11)?,          // last_synced
                row.get::<_, String>(12)?,          // created_at
                row.get::<_, String>(13)?,          // updated_at
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

        // Drop the old devices table
        trans.execute("DROP TABLE devices;", [])?;
        
        // Recreate the devices table with updated schema
        trans.execute(create_table_sql, [])?;

        // Restore the data - exclude id to let autoincrement handle it
        let mut insert_stmt: Statement = trans.prepare(
            "INSERT INTO devices (name, serial_number, mac, device_type, device_status, 
                                status_details, local_ip, public_ip, last_online_at, 
                                device_created_at, group_id, last_synced, created_at, updated_at) 
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)"
        )?;

        for device in exisiting_devices {
            insert_stmt.execute(rusqlite::params![
                device.0,  device.1,  device.2,  device.3,  device.4,  device.5,  device.6,
                device.7,  device.8,  device.9,  device.10, device.11, device.12, device.13,
            ])?;
            info!("Restored device: {}", device.0);
        }
        Ok(())
    }

    pub fn store_access_token(&self, token: &str, expires_at: &str) -> SqlResult<()> {
        let mut conn: Connection = self.get_connection()?;

        let trans: Transaction = conn.transaction()?;

        let current_datetime: String = Utc::now().to_rfc3339();

        match trans.execute(
            "INSERT INTO access_tokens (access_token, expires_at, created_at, updated_at) 
            VALUES (?1, ?2, ?3, ?4)",
            params![token, expires_at, current_datetime, current_datetime],
        ) {
            Ok(_) => {
                trans.commit()?;
                info!("Stored new access token, expires at {}", expires_at);
                Ok(())
            }
            Err(e) => {
                trans.rollback()?;
                // Transaction automatically rolls back on drop if not committed
                error!("Failed to store access token: {}", e);
                Err(e)
            }
        }
    }

    pub fn get_access_token(&self) -> SqlResult<Option<Token>> {
        let conn: Connection = self.get_connection()?;

        let mut stmt: Statement = conn.prepare(
            "SELECT access_token, expires_at, created_at FROM access_tokens 
            ORDER BY created_at DESC LIMIT 1"
        )?;

        let current_token: SqlResult<Token> = stmt.query_one([], |row: &Row<'_>| {
            Ok(Token {
                access_token: row.get(0)?,
                expires_at: row.get(1)?,
                created_at: row.get(2)?,
            })
        });
        Ok(current_token.ok())
    }

    pub fn log_api_call(&self, endpoint: &str, method: &str, status_code: Option<i32>, response_message: Option<&str>) -> SqlResult<()> {
        let mut conn: Connection = self.get_connection()?;
        let trans: Transaction = conn.transaction()?;

        let current_datetime: String = Utc::now().to_rfc3339();

        match trans.execute(
            "INSERT INTO api_logs (endpoint, method, status_code, response_message, created_at) 
            VALUES (?1, ?2, ?3, ?4, ?5)",
            params![endpoint, method, status_code, response_message, current_datetime],
        ) {
            Ok(_) => {
                trans.commit()?;
                info!("Logged API call: {} {}", method, endpoint);
                Ok(())
            }
            Err(e) => {
                trans.rollback()?;
                error!("Failed to log API call: {}", e);
                Err(e)
            }
        }
    }
    
    pub fn store_sites(self, sites_data: Vec<Site>) -> SqlResult<()> {
        let mut conn: Connection = self.get_connection()?;
        let trans: Transaction = conn.transaction()?;
        let current_datetime: String = Utc::now().to_rfc3339();

        for site in &sites_data {
            // Check if site is already existing on the database
            let existing_site: SqlResult<(Option<f32>, Option<f32>)> = trans.query_one(
                "SELECT latitude, longitude FROM sites WHERE groupid = ?1",
                params![site.groupid], {
                    |row: &Row<'_>| Ok((row.get::<_, Option<f32>>(0)?, row.get::<_, Option<f32>>(1)?))
                }
            );

            match existing_site {
                Ok((existing_lat, existing_lon)) => {
                    // Preserve existing coordinates if they exisit and new data is None
                    let latitude = site.latitude.or(existing_lat);
                    let longitude = site.longitude.or(existing_lon);

                    // Update existing site
                    trans.execute(
                        "UPDATE sites 
                            SET name = ?, latitude = ?, longitude = ?, last_synced = ?, updated_at = ?
                            WHERE groupid = ?", params![site.name, latitude, longitude, current_datetime, current_datetime, site.groupid]
                    )?;
                }
                Err(_) => {
                    // Insert new site
                    trans.execute(
                        "INSERT INTO sites (name, groupid, latitude, longitude, last_synced, updated_at)
                            VALUES (?1, ?2, ?3, ?4, ?5, ?6)", 
                            params![site.name, site.groupid, site.latitude, site.longitude, current_datetime, current_datetime]
                    )?;
                }
            }
        }

        match Ok(()) {
            Ok(_) => {
                trans.commit()?;
                info!("Stored/Updated {} sites", sites_data.len());
                Ok(())
            }
            Err(e) => {
                trans.rollback()?;
                error!("Error storing sites: {}", e);
                Err(e)
            }
        }
    }

    pub fn get_sites(&self) -> SqlResult<Vec<Site>> {
        let conn: Connection = self.get_connection()?;

        let mut stmt: Statement = conn.prepare(
            "SELECT name, groupid, latitude, longitude, last_synced, created_at FROM sites"
        )?;

        let sites: Vec<SqlResult<Site>> = stmt.query_map([], |row: &Row<'_>| {
            Ok(Site {
                name: row.get(0)?,
                groupid: row.get(1)?,
                latitude: row.get(2)?,
                longitude: row.get(3)?,
                last_synced: row.get(4)?,
                created_at: row.get(5)?,
                updated_at: None, // Not fetched in this query
            })
        })?
        .collect();

        // Convert Vec<SqlResult<Site>> to SqlResult<Vec<Site>>
        sites.into_iter().collect()
    }

    pub fn get_sites_list(&self) -> SqlResult<Vec<i32>> {
        let conn = self.get_connection()?;

        let mut stmt = conn.prepare(
            "SELECT groupid FROM sites ORDER BY groupid ASC",
        )?;

        // Map rows into Vec<i32>
        let sites_iter = stmt.query_map([], |row: &Row<'_>| {
            Ok(row.get::<_, i32>(0)?)
        })?;

        let mut sites: Vec<i32> = Vec::new();
        for site in sites_iter {
            match site {
                Ok(id) => sites.push(id),
                Err(e) => {
                    error!("Error reading site row: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(sites)
    }

    pub fn store_devices(&self, devices_data: Vec<Device>, group_id: i32) -> SqlResult<()> {
        let mut conn: Connection = self.get_connection()?;
        let mut trans: Transaction = conn.transaction()?;
        let current_datetime: String = Utc::now().to_rfc3339();

        // Delete first existing devices for the group
        trans.execute(
            "DELETE FROM devices WHERE group_id = ?1",
            params![group_id],
        )?;

        // Insert new devices
        let mut insert_stmt: &str = "INSERT INTO devices (name, serial_number, mac, device_type, device_status, 
                                status_details, local_ip, public_ip, last_online_at, 
                                device_created_at, group_id, last_synced, created_at, updated_at) 
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14);";

        for (i, device) in devices_data.iter().enumerate() {
            info!("DEBUG: Storing device {}/{} for group {}", i + 1, devices_data.len(), group_id);
            info!("DEBUG: Device data - Name: '{}', Serial: '{}', MAC: '{}', Type: '{}'", 
                device.name, 
                device.serial_number.as_deref().unwrap_or("N/A"), 
                device.mac.as_deref().unwrap_or("N/A"), 
                device.device_type.as_deref().unwrap_or("N/A")
            );

            match trans.execute(insert_stmt, params![
                device.name,
                device.serial_number,
                device.mac,
                device.device_type,
                device.device_status,
                device.status_details,
                device.local_ip,
                device.public_ip,
                device.last_online_at,
                device.device_created_at,
                group_id,
                current_datetime,
                current_datetime,
                current_datetime,
            ]) {
                Ok(_) => {
                    info!(
                        "DEBUG: Successfully inserted device '{}' with serial '{}'",
                        device.name,
                        device.serial_number.as_deref().unwrap_or("N/A")
                    );
                    Ok(()) // ✅ return success
                }
                Err(e) => {
                    error!(
                        "UNIQUE CONSTRAINT ERROR - Failed to insert device {} for group {}",
                        i + 1,
                        group_id
                    );
                    error!(
                        "DEVICE DETAILS - Name: '{}', Serial: '{}', MAC: '{}'",
                        device.name,
                        device.serial_number.as_deref().unwrap_or("N/A"),
                        device.mac.as_deref().unwrap_or("N/A")
                    );
                    error!("ERROR: {}", e);

                    // Detect constraint error properly
                    if let SqlError::SqliteFailure(err, _) = &e {
                        if err.code == SqlErrorCode::ConstraintViolation {
                            error!(
                                "Constraint violation while inserting device '{:?}': {}",
                                device.serial_number, err
                            );
                        }
                    }

                    Err(e)
                }
            }?;   
        }

        match Ok(()) { 
            Ok(_) => {
                info!("Stored {} devices for group {} successfully:", devices_data.len(), group_id);
                trans.commit()?;
                Ok(())
            }
            Err(e) => {
                trans.rollback()?;
                Err(e)
            }
        }
    }
}