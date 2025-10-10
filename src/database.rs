use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use log::{info, warn, error};
use chrono::{Utc, Duration};
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
use std::error::Error;
use serde::de;

pub struct Token {
    pub access_token: String,
    pub expires_at: String,
    pub created_at: String,
}

pub struct Site {
    pub name: String,
    pub group_id: i32,
    pub latitude: Option<f32>, // REAL is only 8-bit float in SQLite
    pub longitude: Option<f32>,
    pub last_synced: String,
    pub created_at: String,
    pub updated_at: Option<String>,
}

pub struct SitePin {
    pub site: Site,
    pub status: String,
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
    pub updated_at: Option<String>,
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
                group_id INTEGER NOT NULL UNIQUE,
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
                FOREIGN KEY (group_id) REFERENCES sites (group_id)
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
                "SELECT latitude, longitude FROM sites WHERE group_id = ?1",
                params![site.group_id], {
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
                            WHERE group_id = ?", params![site.name, latitude, longitude, current_datetime, current_datetime, site.group_id]
                    )?;
                }
                Err(_) => {
                    // Insert new site
                    trans.execute(
                        "INSERT INTO sites (name, group_id, latitude, longitude, last_synced, updated_at)
                            VALUES (?1, ?2, ?3, ?4, ?5, ?6)", 
                            params![site.name, site.group_id, site.latitude, site.longitude, current_datetime, current_datetime]
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
            "SELECT name, group_id, latitude, longitude, last_synced, created_at FROM sites"
        )?;

        let sites: Vec<SqlResult<Site>> = stmt.query_map([], |row: &Row<'_>| {
            Ok(Site {
                name: row.get(0)?,
                group_id: row.get(1)?,
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
            "SELECT group_id FROM sites ORDER BY group_id ASC",
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
        let trans: Transaction = conn.transaction()?;
        let current_datetime: String = Utc::now().to_rfc3339();

        // Delete first existing devices for the group
        trans.execute(
            "DELETE FROM devices WHERE group_id = ?1",
            params![group_id],
        )?;

        // Insert new devices
        let insert_stmt: &str = "INSERT INTO devices (name, serial_number, mac, device_type, device_status, 
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

    pub fn get_devices(&self, group_id: Option<i32>) -> SqlResult<Vec<Device>> {
        let conn = self.get_connection()?;

        let sql = if group_id.is_some() {
            "SELECT name, serial_number, mac, device_type, device_status, status_details, 
                    local_ip, public_ip, last_online_at, device_created_at, group_id, 
                    last_synced, created_at, updated_at
            FROM devices WHERE group_id = ?1"
        } else {
            "SELECT name, serial_number, mac, device_type, device_status, status_details, 
                    local_ip, public_ip, last_online_at, device_created_at, group_id, 
                    last_synced, created_at, updated_at
            FROM devices ORDER BY name ASC"
        };

        let mut stmt = conn.prepare(sql)?;

        // Collect into Vec<Device> immediately so we don’t return iterators with short lifetimes
        let devices: Vec<Device> = if let Some(gid) = group_id {
            stmt.query_map(params![gid], |row| {
                Ok(Device {
                    id: 0, // Not fetched
                    name: row.get(0)?,
                    serial_number: row.get(1)?,
                    mac: row.get(2)?,
                    device_type: row.get(3)?,
                    device_status: row.get(4)?,
                    status_details: row.get(5)?,
                    local_ip: row.get(6)?,
                    public_ip: row.get(7)?,
                    last_online_at: row.get(8)?,
                    device_created_at: row.get(9)?,
                    group_id: row.get(10)?,
                    last_synced: row.get(11)?,
                    created_at: row.get(12)?,
                    updated_at: row.get(13).ok(),
                })
            })?
            .filter_map(Result::ok)
            .collect()
        } else {
            stmt.query_map([], |row| {
                Ok(Device {
                    id: 0,
                    name: row.get(0)?,
                    serial_number: row.get(1)?,
                    mac: row.get(2)?,
                    device_type: row.get(3)?,
                    device_status: row.get(4)?,
                    status_details: row.get(5)?,
                    local_ip: row.get(6)?,
                    public_ip: row.get(7)?,
                    last_online_at: row.get(8)?,
                    device_created_at: row.get(9)?,
                    group_id: row.get(10)?,
                    last_synced: row.get(11)?,
                    created_at: row.get(12)?,
                    updated_at: row.get(13).ok(),
                })
            })?
            .filter_map(Result::ok)
            .collect()
        };

        info!(
            "Fetched {} devices{}",
            devices.len(),
            if let Some(gid) = group_id {
                format!(" for group {}", gid)
            } else {
                "".to_string()
            }
        );

        match Ok(()) {
            Ok(_) => Ok(devices),
            Err(e) => {
                error!("Failed to fetch devices{}: {}", if let Some(gid) = group_id { format!(" for group {}", gid) } else { "".to_string() }, e);
                Err(e)
            }
        }
    }

    pub fn get_sites_with_devices(&self) -> SqlResult<Vec<(Site, Vec<Device>)>> {
        let conn = self.get_connection()?;

        let mut stmt = conn.prepare(
            "SELECT name, group_id, latitude, longitude, last_synced, created_at FROM sites",
        )?;

        let mut sites_with_devices: Vec<(Site, Vec<Device>)> = Vec::new();

        let site_rows = stmt.query_map([], |row| {
            Ok(Site {
                name: row.get(0)?,
                group_id: row.get(1)?,
                latitude: row.get(2)?,
                longitude: row.get(3)?,
                last_synced: row.get(4)?,
                created_at: row.get(5)?,
                updated_at: None,
            })
        })?;

        // Iterate over each site, fetch devices for its group_id
        for site_result in site_rows {
            match site_result {
                Ok(site) => {
                    let devices = self.get_devices(Some(site.group_id))?;
                    sites_with_devices.push((site, devices));
                }
                Err(e) => {
                    error!("Error mapping site row: {}", e);
                    return Err(e);
                }
            }
        }

        info!("Successfully fetched {} sites with devices", sites_with_devices.len());
        Ok(sites_with_devices)
    }

    pub fn cleanup_old_logs(&self, days: Option<u32>) -> SqlResult<()> {
        let cutoff_date = (Utc::now() - Duration::days(days.unwrap_or(30) as i64)).to_rfc3339(); // Default to 30 days if not specified
        let mut conn: Connection = self.get_connection()?;
        let trans: Transaction = conn.transaction()?;

        // Perform the cleanup operation
        let delete_logs = trans.execute(
            "DELETE FROM api_logs WHERE created_at < ?1",
            params![cutoff_date],
        )?;

        match delete_logs {
            0 => info!("No old logs to clean up."),
            n => info!("Cleaned up {} old log entries older than {}", n, cutoff_date),
            
        }

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    pub fn sync_sites_pins(&self) -> SqlResult<()> {
        let mut conn: Connection = self.get_connection()?;
        let trans: Transaction = conn.transaction()?;

        {
            // Get all sites
            let mut stmt = trans.prepare(
                "SELECT name, group_id, latitude, longitude FROM sites"
            )?;

            struct TempSite {
                name: String,
                group_id: i32,
                latitude: Option<f32>,
                longitude: Option<f32>,
            }

            let site_rows = stmt.query_map([], |row| {
                Ok(TempSite {
                    name: row.get(0)?,
                    group_id: row.get(1)?,
                    latitude: row.get(2)?,
                    longitude: row.get(3)?,
                })
            })?;

            for row in site_rows {
                let site = row?;
                let group_id = site.group_id;
                let site_lat = site.latitude;
                let site_lon = site.longitude;
                let name = site.name;

                let status = self._calculate_site_status(group_id)?;
                let current_datetime: String = Utc::now().to_rfc3339();

                // Try to fetch existing site pin
                let existing_pin: SqlResult<(Option<f32>, Option<f32>, String)> = trans.query_row(
                    "SELECT latitude, longitude, created_at FROM site_pins WHERE group_id = ?1",
                    params![group_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                );

                match existing_pin {
                    Ok((existing_lat, existing_lon, _created_at)) => {
                        // Use existing coordinates if they exist
                        let final_lat = existing_lat.or(site_lat);
                        let final_lon = existing_lon.or(site_lon);

                        trans.execute(
                            "UPDATE site_pins
                            SET name = ?1, latitude = ?2, longitude = ?3, status = ?4,
                                last_synced = ?5, updated_at = ?6
                            WHERE group_id = ?7",
                            params![
                                name,
                                final_lat,
                                final_lon,
                                status,
                                current_datetime,
                                current_datetime,
                                group_id
                            ],
                        )?;
                    }
                    Err(e) => {
                        // Create new pin
                        trans.execute(
                            "INSERT INTO site_pins
                            (group_id, name, latitude, longitude, status, last_synced, created_at, updated_at)
                            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                            params![
                                group_id,
                                name,
                                site_lat,
                                site_lon,
                                status,
                                current_datetime,
                                current_datetime,
                                current_datetime
                            ],
                        )?;
                    }
                }
            }

            let count: i32 = trans.query_row("SELECT COUNT(*) FROM site_pins", [], |row| row.get(0))?;
            info!("Synced {} site pins successfully (preserving manual coordinates)", count);
        }

        // Commit after all successful updates
        trans.commit()?;
        Ok(())
    }


    fn _calculate_site_status(&self, group_id: i32) -> SqlResult<String> {
        let conn: Connection = self.get_connection()?;
        // Query all device statuses for the given group
        let mut stmt = match conn.prepare(
            "SELECT device_status FROM devices WHERE group_id = ?1"
        ) {
            Ok(stmt) => stmt,
            Err(e) => {
                error!("Error preparing statement for group {}: {}", group_id, e);
                return Ok("UNKNOWN".to_string());
            }
        };

        let rows = stmt.query_map(params![group_id], |row| row.get::<_, Option<String>>(0));

        let device_statuses: Vec<String> = match rows {
            Ok(iter) => iter.filter_map(|r| r.ok().flatten()).collect(),
            Err(e) => {
                error!("Error fetching device statuses for group {}: {}", group_id, e);
                return Ok("UNKNOWN".to_string());
            }
        };

        // If no devices → OFF
        if device_statuses.is_empty() {
            return Ok("OFF".to_string());
        }

        // Count statuses
        let mut online_count: usize = 0;
        let mut offline_count: usize = 0;

        for status in device_statuses.iter() {
            let s = status.to_uppercase();
            if ["ONLINE", "ON"].contains(&s.as_str()) {
                online_count += 1;
            } else if ["OFFLINE", "OFF", "NEVER_ONLINE"].contains(&s.as_str()) {
                offline_count += 1;
            }
        }

        let total_count = device_statuses.len();

        // Determine overall site status
        let status = if online_count == total_count {
            "ONLINE".to_string()
        } else if offline_count == total_count {
            "UNKNOWN".to_string()
        } else if offline_count > 0 {
            "WARNING".to_string()
        } else {
            "WARNING".to_string()
        };
        
        Ok(status)
    }

    pub fn get_site_pins(&self) -> SqlResult<Vec<SitePin>> {
        let conn: Connection = self.get_connection()?;
        let mut stmt: Statement<'_> = conn.prepare(
            "SELECT group_id, name, latitude, longitude, status, last_synced, created_at, updated_at 
                FROM site_pins
                ORDER BY name ASC
                ",
        )?;
        let pins = stmt.query_map(params![], |row| {
            Ok(SitePin {
                site: Site {
                    group_id: row.get::<_, i32>(0)?,               // group_id
                    name: row.get::<_, String>(1)?,            // name
                    latitude: row.get::<_, Option<f32>>(2)?,      // latitude
                    longitude: row.get::<_, Option<f32>>(3)?,      // longitude
                    last_synced: row.get::<_, String>(5)?,            // last_synced
                    created_at: row.get::<_, String>(6)?,            // created_at
                    updated_at: row.get::<_, Option<String>>(7)?,    // updated_at
                },
                status: row.get::<_, String>(4)?,            // status
            })
        })?.collect::<SqlResult<Vec<_>>>()?;

        Ok(pins)
    }

    pub fn update_site_pin_coordinates(&self, group_id: i32, latitude: Option<f32>, longitude: Option<f32>) -> SqlResult<()> {
        let mut conn: Connection = self.get_connection()?;
        if latitude.is_none() || longitude.is_none() {
            warn!("No coordinates provided to update for group_id {}", group_id);
            return Err(SqlError::InvalidQuery); // Or a more specific error
        }

        let latitude = latitude.unwrap();
        let longitude = longitude.unwrap();

        conn.execute(
            "UPDATE site_pins SET latitude = ?1, longitude = ?2 WHERE group_id = ?3",
            params![latitude, longitude, group_id],
        )?;

        Ok(())
    }
}