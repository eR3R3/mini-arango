use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use crate::common::error::{ArangoError, Result};

/// HTTP request representation
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub remote_addr: Option<SocketAddr>,
    pub query_params: HashMap<String, String>,
}

impl HttpRequest {
    pub fn new(method: HttpMethod, path: String) -> Self {
        HttpRequest {
            method,
            path,
            headers: HashMap::new(),
            body: Vec::new(),
            remote_addr: None,
            query_params: HashMap::new(),
        }
    }
    
    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }
    
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }
    
    pub fn with_remote_addr(mut self, addr: SocketAddr) -> Self {
        self.remote_addr = Some(addr);
        self
    }
    
    pub fn get_header(&self, key: &str) -> Option<&String> {
        self.headers.get(key)
    }
    
    pub fn get_query_param(&self, key: &str) -> Option<&String> {
        self.query_params.get(key)
    }
    
    pub fn body_as_string(&self) -> Result<String> {
        String::from_utf8(self.body.clone())
            .map_err(|e| ArangoError::bad_parameter(format!("Invalid UTF-8 in body: {}", e)))
    }
    
    pub fn content_type(&self) -> Option<&String> {
        self.get_header("content-type")
    }
    
    pub fn content_length(&self) -> usize {
        self.body.len()
    }
}

/// HTTP methods
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpMethod {
    GET,
    POST,
    PUT,
    DELETE,
    PATCH,
    HEAD,
    OPTIONS,
}

impl HttpMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            HttpMethod::GET => "GET",
            HttpMethod::POST => "POST",
            HttpMethod::PUT => "PUT",
            HttpMethod::DELETE => "DELETE",
            HttpMethod::PATCH => "PATCH",
            HttpMethod::HEAD => "HEAD",
            HttpMethod::OPTIONS => "OPTIONS",
        }
    }
}

/// HTTP response representation
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status_code: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn new(status_code: u16) -> Self {
        HttpResponse {
            status_code,
            headers: HashMap::new(),
            body: Vec::new(),
        }
    }
    
    pub fn ok() -> Self {
        Self::new(200)
    }
    
    pub fn not_found() -> Self {
        Self::new(404)
    }
    
    pub fn internal_server_error() -> Self {
        Self::new(500)
    }
    
    pub fn bad_request() -> Self {
        Self::new(400)
    }
    
    pub fn unauthorized() -> Self {
        Self::new(401)
    }
    
    pub fn forbidden() -> Self {
        Self::new(403)
    }
    
    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }
    
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }
    
    pub fn with_json_body<T: Serialize>(mut self, data: &T) -> Result<Self> {
        let json = serde_json::to_vec(data)
            .map_err(|e| ArangoError::internal(format!("Failed to serialize JSON: {}", e)))?;
        self.body = json;
        self.headers.insert("content-type".to_string(), "application/json".to_string());
        Ok(self)
    }
    
    pub fn with_text_body(mut self, text: String) -> Self {
        self.body = text.into_bytes();
        self.headers.insert("content-type".to_string(), "text/plain".to_string());
        self
    }
    
    pub fn get_header(&self, key: &str) -> Option<&String> {
        self.headers.get(key)
    }
    
    pub fn body_as_string(&self) -> Result<String> {
        String::from_utf8(self.body.clone())
            .map_err(|e| ArangoError::internal(format!("Invalid UTF-8 in response body: {}", e)))
    }
    
    pub fn content_length(&self) -> usize {
        self.body.len()
    }
    
    pub fn is_success(&self) -> bool {
        self.status_code >= 200 && self.status_code < 300
    }
    
    pub fn is_error(&self) -> bool {
        self.status_code >= 400
    }
}

/// Connection statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionStats {
    pub total_connections: u64,
    pub active_connections: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub requests_received: u64,
    pub responses_sent: u64,
    pub connection_errors: u64,
    pub request_errors: u64,
    pub average_request_time: Duration,
    pub peak_connections: u64,
}

impl Default for ConnectionStats {
    fn default() -> Self {
        ConnectionStats {
            total_connections: 0,
            active_connections: 0,
            bytes_received: 0,
            bytes_sent: 0,
            requests_received: 0,
            responses_sent: 0,
            connection_errors: 0,
            request_errors: 0,
            average_request_time: Duration::default(),
            peak_connections: 0,
        }
    }
}

/// Network manager for tracking connections and statistics
pub struct NetworkManager {
    stats: Arc<RwLock<ConnectionStats>>,
    request_times: Arc<RwLock<Vec<Duration>>>,
    max_request_time_samples: usize,
}

impl NetworkManager {
    pub fn new() -> Self {
        NetworkManager {
            stats: Arc::new(RwLock::new(ConnectionStats::default())),
            request_times: Arc::new(RwLock::new(Vec::new())),
            max_request_time_samples: 1000,
        }
    }
    
    /// Record a new connection
    pub fn record_connection(&self) {
        let mut stats = self.stats.write();
        stats.total_connections += 1;
        stats.active_connections += 1;
        if stats.active_connections > stats.peak_connections {
            stats.peak_connections = stats.active_connections;
        }
    }
    
    /// Record a connection being closed
    pub fn record_connection_closed(&self) {
        let mut stats = self.stats.write();
        if stats.active_connections > 0 {
            stats.active_connections -= 1;
        }
    }
    
    /// Record a connection error
    pub fn record_connection_error(&self) {
        let mut stats = self.stats.write();
        stats.connection_errors += 1;
    }
    
    /// Record bytes received
    pub fn record_bytes_received(&self, bytes: u64) {
        let mut stats = self.stats.write();
        stats.bytes_received += bytes;
    }
    
    /// Record bytes sent
    pub fn record_bytes_sent(&self, bytes: u64) {
        let mut stats = self.stats.write();
        stats.bytes_sent += bytes;
    }
    
    /// Record a request received
    pub fn record_request(&self) {
        let mut stats = self.stats.write();
        stats.requests_received += 1;
    }
    
    /// Record a response sent
    pub fn record_response(&self, request_time: Duration, is_error: bool) {
        let mut stats = self.stats.write();
        stats.responses_sent += 1;
        
        if is_error {
            stats.request_errors += 1;
        }
        
        drop(stats);
        
        // Update request times
        {
            let mut times = self.request_times.write();
            times.push(request_time);
            
            // Keep only the last N samples
            if times.len() > self.max_request_time_samples {
                times.remove(0);
            }
        }
        
        // Update average request time
        self.update_average_request_time();
    }
    
    /// Get current statistics
    pub fn get_stats(&self) -> ConnectionStats {
        self.stats.read().clone()
    }
    
    /// Reset all statistics
    pub fn reset_stats(&self) {
        let mut stats = self.stats.write();
        *stats = ConnectionStats::default();
        
        let mut times = self.request_times.write();
        times.clear();
    }
    
    fn update_average_request_time(&self) {
        let times = self.request_times.read();
        if !times.is_empty() {
            let total: Duration = times.iter().sum();
            let average = total / times.len() as u32;
            
            drop(times);
            
            let mut stats = self.stats.write();
            stats.average_request_time = average;
        }
    }
}

impl Default for NetworkManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Request timing helper
pub struct RequestTimer {
    start_time: Instant,
}

impl RequestTimer {
    pub fn new() -> Self {
        RequestTimer {
            start_time: Instant::now(),
        }
    }
    
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

impl Default for RequestTimer {
    fn default() -> Self {
        Self::new()
    }
}

/// Client connection information
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub remote_addr: SocketAddr,
    pub user_agent: Option<String>,
    pub connected_at: Instant,
    pub request_count: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

impl ClientInfo {
    pub fn new(remote_addr: SocketAddr) -> Self {
        ClientInfo {
            remote_addr,
            user_agent: None,
            connected_at: Instant::now(),
            request_count: 0,
            bytes_sent: 0,
            bytes_received: 0,
        }
    }
    
    pub fn with_user_agent(mut self, user_agent: String) -> Self {
        self.user_agent = Some(user_agent);
        self
    }
    
    pub fn connection_duration(&self) -> Duration {
        self.connected_at.elapsed()
    }
    
    pub fn record_request(&mut self, bytes_received: u64, bytes_sent: u64) {
        self.request_count += 1;
        self.bytes_received += bytes_received;
        self.bytes_sent += bytes_sent;
    }
}

/// URL parsing utilities
pub struct UrlUtils;

impl UrlUtils {
    /// Parse query string into key-value pairs
    pub fn parse_query_string(query: &str) -> HashMap<String, String> {
        let mut params = HashMap::new();
        
        if query.is_empty() {
            return params;
        }
        
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                params.insert(
                    urlencoding::decode(key).unwrap_or_default().to_string(),
                    urlencoding::decode(value).unwrap_or_default().to_string(),
                );
            } else {
                params.insert(
                    urlencoding::decode(pair).unwrap_or_default().to_string(),
                    String::new(),
                );
            }
        }
        
        params
    }
    
    /// Extract path and query from a URL
    pub fn parse_path_and_query(url: &str) -> (String, HashMap<String, String>) {
        if let Some((path, query)) = url.split_once('?') {
            (path.to_string(), Self::parse_query_string(query))
        } else {
            (url.to_string(), HashMap::new())
        }
    }
} 