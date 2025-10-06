use actix_web::{web, App, HttpServer, Responder};


#[actix_web::get("/greet/{name}")]
async fn greet(name: web::Path<String>) -> impl Responder {
    format!("{name} Gae")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let port: u16 = 8080;
    println!("Server is running on port {port}");

    HttpServer::new(|| App::new()
        .service(greet))
        .bind(("127.0.0.1", port))?
        .workers(4)
        .run()
        .await
}

