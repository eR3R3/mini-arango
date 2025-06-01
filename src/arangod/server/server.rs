use std::sync::Arc;
use std::net::SocketAddr;
use serde::{Deserialize, Serialize};
use crate::common::error::{ArangoError, Result};
use crate::arangod::database::Database;

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
    pub host: String,
    pub max_connections: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            port: 8529,
            host: "127.0.0.1".to_string(),
            max_connections: 1000,
        }
    }
}

/// HTTP Server
pub struct Server {
    database: Arc<Database>,
    config: ServerConfig,
}

impl Server {
    /// Create a new server
    pub fn new(database: Database) -> Self {
        Server {
            database: Arc::new(database),
            config: ServerConfig::default(),
        }
    }
    
    /// Set the port
    pub fn with_port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }
    
    /// Set the host
    pub fn with_host(mut self, host: String) -> Self {
        self.config.host = host;
        self
    }
    
    /// Start the server
    pub async fn start(&self) -> Result<()> {
        let addr: SocketAddr = format!("{}:{}", self.config.host, self.config.port)
            .parse()
            .map_err(|e| ArangoError::internal(format!("Invalid address: {}", e)))?;
        
        tracing::info!("Starting server on {}", addr);
        
        // For now, just return Ok - in a real implementation this would start the HTTP server
        // Using axum or similar framework
        Ok(())
    }
    
    /// Get server config
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }
    
    /// Get database reference
    pub fn database(&self) -> &Arc<Database> {
        &self.database
    }
} 