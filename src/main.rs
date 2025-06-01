use std::path::PathBuf;
use std::io::{self, Write};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;
use clap::{Parser, Subcommand, Args};
use serde_json::{json, Value, Map};
use mini_arangodb::{
    ArangoError, Result, Document, DocumentKey, DocumentId, DocumentFilter,
    OptimisticRocksDBEngine, StorageEngine
};
use mini_arangodb::arangod::storage_engine::optimistic_rocksdb_engine::CollectionType;

/// Mini ArangoDB - A Rust implementation of ArangoDB with RocksDB backend
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Database data directory
    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start an interactive shell
    Shell,
    /// Collection management commands
    Collection(CollectionCommand),
    /// Document management commands  
    Document(DocumentCommand),
    /// Database management commands
    Database(DatabaseCommand),
    /// Import/Export operations
    Import(ImportCommand),
    /// Graph operations
    Graph(GraphCommand),
    /// Server operations
    Server(ServerCommand),
}

#[derive(Args)]
struct CollectionCommand {
    #[command(subcommand)]
    action: CollectionAction,
}

#[derive(Subcommand)]
enum CollectionAction {
    /// Create a new collection
    Create {
        /// Collection name
        name: String,
        /// Collection type (document or edge)
        #[arg(short, long, default_value = "document")]
        collection_type: String,
    },
    /// Drop a collection
    Drop {
        /// Collection name
        name: String,
        /// Force drop without confirmation
        #[arg(short, long)]
        force: bool,
    },
    /// List all collections
    List,
    /// Show collection information
    Info {
        /// Collection name
        name: String,
    },
}

#[derive(Args)]
struct DocumentCommand {
    /// Collection name
    #[arg(short, long)]
    collection: String,

    #[command(subcommand)]
    action: DocumentAction,
}

#[derive(Subcommand)]
enum DocumentAction {
    /// Insert a new document
    Insert {
        /// Document data as JSON string
        data: String,
        /// Document key (optional, will be generated if not provided)
        #[arg(short, long)]
        key: Option<String>,
    },
    /// Get a document by key
    Get {
        /// Document key
        key: String,
    },
    /// Update a document
    Update {
        /// Document key
        key: String,
        /// Updated document data as JSON string
        data: String,
    },
    /// Delete a document
    Delete {
        /// Document key
        key: String,
    },
    /// Find documents (simple query)
    Find {
        /// Optional filter as JSON string
        #[arg(short, long)]
        filter: Option<String>,
        /// Maximum number of results
        #[arg(short, long)]
        limit: Option<usize>,
    },
    /// Count documents in collection
    Count,
}

#[derive(Args)]
struct DatabaseCommand {
    #[command(subcommand)]
    action: DatabaseAction,
}

#[derive(Subcommand)]
enum DatabaseAction {
    /// Show database statistics
    Stats,
    /// Compact the database
    Compact,
    /// Initialize a new database
    Init,
}

#[derive(Args)]
struct ImportCommand {
    /// File to import from
    file: PathBuf,
    /// Target collection
    #[arg(short, long)]
    collection: String,
    /// File format (json, jsonl, csv)
    #[arg(short, long, default_value = "jsonl")]
    format: String,
}

#[derive(Args)]
struct GraphCommand {
    #[command(subcommand)]
    action: GraphAction,
}

#[derive(Subcommand)]
enum GraphAction {
    /// Create a simple social network graph demo
    Demo,
    /// Create vertex and edge collections for graph
    Setup {
        /// Graph name
        name: String,
        /// Vertex collection name
        #[arg(short, long, default_value = "vertices")]
        vertex_collection: String,
        /// Edge collection name
        #[arg(short, long, default_value = "edges")]
        edge_collection: String,
    },
    /// Insert a vertex into the graph
    InsertVertex {
        /// Vertex collection name
        collection: String,
        /// Vertex data as JSON
        data: String,
        /// Vertex key (optional)
        #[arg(short, long)]
        key: Option<String>,
    },
    /// Insert an edge into the graph
    InsertEdge {
        /// Edge collection name
        collection: String,
        /// Source vertex ID (collection/key format)
        from: String,
        /// Target vertex ID (collection/key format)
        to: String,
        /// Edge data as JSON (optional)
        #[arg(short, long, default_value = "{}")]
        data: String,
    },
    /// Traverse from a vertex
    Traverse {
        /// Starting vertex ID (collection/key format)
        start: String,
        /// Maximum depth
        #[arg(short, long, default_value = "3")]
        depth: usize,
    },
}

#[derive(Args)]
struct ServerCommand {
    #[command(subcommand)]
    action: ServerAction,
}

#[derive(Subcommand)]
enum ServerAction {
    /// Start the database server
    Start {
        /// Server port
        #[arg(short, long, default_value = "8529")]
        port: u16,
        /// Bind address
        #[arg(short, long, default_value = "127.0.0.1")]
        bind: String,
    },
    /// Stop the database server
    Stop,
    /// Show server status
    Status,
}

/// 图遍历结果中的顶点信息
#[derive(Debug, Clone)]
struct VertexInfo {
    vertex_id: String,
    vertex_data: String,
    edge_info: Option<EdgeInfo>,
}

/// 边信息
#[derive(Debug, Clone)]
struct EdgeInfo {
    from: String,
    to: String,
    edge_data: String,
}

/// 图遍历算法实现
fn traverse_graph(
    engine: &OptimisticRocksDBEngine,
    start_vertex: &str,
    max_depth: usize
) -> Result<Vec<Vec<VertexInfo>>> {
    let mut results = Vec::new();
    let mut visited = HashSet::new();
    let mut current_level = vec![start_vertex.to_string()];

    // 获取所有边集合
    let collections = engine.list_collections();
    let edge_collections: Vec<_> = collections.iter()
        .filter(|c| c.collection_type == CollectionType::Edge)
        .collect();

    visited.insert(start_vertex.to_string());

    for depth in 0..=max_depth {
        let mut level_vertices = Vec::new();
        let mut next_level = Vec::new();

        for vertex_id in &current_level {
            // 获取当前顶点信息
            let vertex_parts: Vec<&str> = vertex_id.split('/').collect();
            if vertex_parts.len() == 2 {
                let collection = vertex_parts[0];
                let key = DocumentKey::new(vertex_parts[1])?;

                if let Some(document) = engine.get_document(collection, &key)? {
                    let vertex_info = VertexInfo {
                        vertex_id: vertex_id.clone(),
                        vertex_data: format!("{}", serde_json::to_string(&document.to_json()).unwrap_or_else(|_| "{}".to_string())),
                        edge_info: None,
                    };
                    level_vertices.push(vertex_info);
                }

                // 如果不是最后一层，寻找连接的顶点
                if depth < max_depth {
                    for edge_collection in &edge_collections {
                        if let Ok(edge_documents) = get_all_documents_in_collection(engine, &edge_collection.name) {
                            for edge_doc in edge_documents {
                                let from = edge_doc.get("_from").and_then(|v| v.as_str()).unwrap_or("");
                                let to = edge_doc.get("_to").and_then(|v| v.as_str()).unwrap_or("");

                                let target_vertex = if from == vertex_id {
                                    Some(to.to_string())
                                } else if to == vertex_id {
                                    Some(from.to_string())
                                } else {
                                    None
                                };

                                if let Some(target) = target_vertex {
                                    if !visited.contains(&target) {
                                        visited.insert(target.clone());
                                        next_level.push(target.clone());

                                        // 获取目标顶点信息
                                        let target_parts: Vec<&str> = target.split('/').collect();
                                        if target_parts.len() == 2 {
                                            let target_collection = target_parts[0];
                                            let target_key = DocumentKey::new(target_parts[1])?;

                                            if let Some(target_document) = engine.get_document(target_collection, &target_key)? {
                                                let edge_info = EdgeInfo {
                                                    from: from.to_string(),
                                                    to: to.to_string(),
                                                    edge_data: format!("{}",
                                                                       serde_json::to_string(&edge_doc.to_json())
                                                                           .unwrap_or_else(|_| "{}".to_string())
                                                    ),
                                                };

                                                let connected_vertex_info = VertexInfo {
                                                    vertex_id: target.clone(),
                                                    vertex_data: format!("{}",
                                                                         serde_json::to_string(&target_document.to_json())
                                                                             .unwrap_or_else(|_| "{}".to_string())
                                                    ),
                                                    edge_info: Some(edge_info),
                                                };
                                                level_vertices.push(connected_vertex_info);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if !level_vertices.is_empty() {
            results.push(level_vertices);
        }

        if next_level.is_empty() {
            break;
        }

        current_level = next_level;
    }

    Ok(results)
}

/// 获取集合中的所有文档（临时实现，用于图遍历）
fn get_all_documents_in_collection(engine: &OptimisticRocksDBEngine, collection_name: &str) -> Result<Vec<Document>> {
    engine.get_all_documents_in_collection(collection_name)
}

struct DatabaseContext {
    engine: OptimisticRocksDBEngine,
    verbose: bool,
}

impl DatabaseContext {
    fn new(data_dir: PathBuf, verbose: bool) -> Result<Self> {
        let engine = OptimisticRocksDBEngine::new(data_dir)?;
        Ok(DatabaseContext { engine, verbose })
    }

    fn log(&self, message: &str) {
        if self.verbose {
            println!("[INFO] {}", message);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize database context
    let ctx = DatabaseContext::new(cli.data_dir, cli.verbose)?;
    ctx.log("Database initialized successfully");

    match cli.command {
        Commands::Shell => run_interactive_shell(ctx).await,
        Commands::Collection(cmd) => handle_collection_command(ctx, cmd).await,
        Commands::Document(cmd) => handle_document_command(ctx, cmd).await,
        Commands::Database(cmd) => handle_database_command(ctx, cmd).await,
        Commands::Import(cmd) => handle_import_command(ctx, cmd).await,
        Commands::Graph(cmd) => handle_graph_command(ctx, cmd).await,
        Commands::Server(cmd) => handle_server_command(ctx, cmd).await,
    }
}

async fn run_interactive_shell(ctx: DatabaseContext) -> Result<()> {
    println!("Welcome to Mini ArangoDB Interactive Shell");
    println!("Type 'help' for available commands, 'exit' to quit");

    loop {
        print!("arangodb> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        match input {
            "exit" | "quit" => {
                println!("Goodbye!");
                break;
            }
            "help" => print_shell_help(),
            "collections" => {
                let collections = ctx.engine.list_collections();
                println!("Collections:");
                for collection in collections {
                    println!("  - {} ({})", collection.name, collection.collection_type.as_str());
                }
            }
            _ => {
                if let Err(e) = execute_shell_command(&ctx, input).await {
                    println!("Error: {}", e);
                }
            }
        }
    }

    Ok(())
}

async fn execute_shell_command(ctx: &DatabaseContext, input: &str) -> Result<()> {
    let start_time = Instant::now();

    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(());
    }

    let result = match parts[0] {
        // 集合管理
        "create" if parts.len() >= 3 && parts[1] == "collection" => {
            let collection_name = parts[2];
            let collection_type = if parts.len() > 3 { parts[3] } else { "document" };
            let ctype = parse_collection_type(collection_type)?;

            ctx.engine.create_collection(collection_name, ctype)?;
            println!("✅ Collection '{}' created successfully", collection_name);
            Ok(())
        }

        "drop" if parts.len() >= 3 && parts[1] == "collection" => {
            let collection_name = parts[2];
            println!("❌ Collection drop not yet implemented for '{}'", collection_name);
            Ok(())
        }

        "info" if parts.len() >= 2 => {
            let collection_name = parts[1];
            if let Some(collection) = ctx.engine.get_collection(collection_name) {
                println!("📋 Collection Information:");
                println!("  Name: {}", collection.name);
                println!("  Type: {}", collection.collection_type.as_str());
                println!("  Documents: {}", collection.document_count);
                println!("  Data size: {} bytes", collection.data_size);
                println!("  Created: {}", collection.created);
            } else {
                println!("❌ Collection '{}' not found", collection_name);
            }
            Ok(())
        }

        // 文档操作
        "insert" if parts.len() >= 3 => {
            let collection_name = parts[1];
            let json_data = parts[2..].join(" ");

            let mut data: Map<String, Value> = serde_json::from_str(&json_data)
                .map_err(|e| ArangoError::bad_parameter(format!("Invalid JSON: {}", e)))?;

            // 如果JSON中没有_key，则根据name字段生成一个友好的key
            if !data.contains_key("_key") {
                if let Some(name_value) = data.get("name") {
                    if let Some(name_str) = name_value.as_str() {
                        let friendly_key = name_str.to_lowercase()
                            .replace(" ", "")
                            .chars()
                            .filter(|c| c.is_alphanumeric())
                            .collect::<String>();
                        if !friendly_key.is_empty() {
                            data.insert("_key".to_string(), Value::String(friendly_key));
                        }
                    }
                }
            }

            let document = Document::new(collection_name, data)?;
            ctx.engine.insert_document(collection_name, &document)?;
            println!("✅ Document inserted with key: {}", document.key());
            Ok(())
        }

        "get" if parts.len() >= 3 => {
            let collection_name = parts[1];
            let key = DocumentKey::new(parts[2])?;

            if let Some(document) = ctx.engine.get_document(collection_name, &key)? {
                println!("{}", serde_json::to_string_pretty(&document.to_json()).unwrap());
            } else {
                println!("❌ Document not found");
            }
            Ok(())
        }

        "update" if parts.len() >= 4 => {
            let collection_name = parts[1];
            let key = DocumentKey::new(parts[2])?;
            let json_data = parts[3..].join(" ");

            if let Some(mut document) = ctx.engine.get_document(collection_name, &key)? {
                let update_data: Map<String, Value> = serde_json::from_str(&json_data)
                    .map_err(|e| ArangoError::bad_parameter(format!("Invalid JSON: {}", e)))?;

                document.merge(update_data);
                ctx.engine.update_document(collection_name, &document)?;
                println!("✅ Document updated successfully");
            } else {
                println!("❌ Document not found");
            }
            Ok(())
        }

        "delete" if parts.len() >= 3 => {
            let collection_name = parts[1];
            let key = DocumentKey::new(parts[2])?;

            if ctx.engine.delete_document(collection_name, &key)? {
                println!("✅ Document deleted successfully");
            } else {
                println!("❌ Document not found");
            }
            Ok(())
        }

        "count" if parts.len() >= 2 => {
            let collection_name = parts[1];
            let count = ctx.engine.count_documents(collection_name)?;
            println!("📊 Document count: {}", count);
            Ok(())
        }

        "show" | "docs" if parts.len() >= 2 => {
            let collection_name = parts[1];
            let limit = parts.get(2).and_then(|s| s.parse::<usize>().ok()).unwrap_or(10);

            println!("📄 Documents in collection '{}':", collection_name);

            let documents = get_all_documents_in_collection(&ctx.engine, collection_name)?;

            if documents.is_empty() {
                println!("  (empty collection)");
            } else {
                let total = documents.len();
                let show_count = std::cmp::min(total, limit);

                for (i, doc) in documents.iter().take(show_count).enumerate() {
                    println!("  📋 Document {} (key: {}):", i + 1, doc.key());
                    let json_str = serde_json::to_string_pretty(&doc.to_json()).unwrap();
                    // Indent each line
                    for line in json_str.lines() {
                        println!("     {}", line);
                    }
                    if i < show_count - 1 {
                        println!();
                    }
                }

                if total > limit {
                    println!("\n  ... and {} more documents (showing {} of {})",
                             total - limit, limit, total);
                    println!("  💡 Use 'show {} <limit>' to see more", collection_name);
                }
            }
            Ok(())
        }

        // 图操作
        "graph" if parts.len() >= 2 => {
            match parts[1] {
                "demo" => {
                    println!("🔗 Creating Social Network Graph Demo");
                    println!("====================================");

                    // Create collections
                    let _users = ctx.engine.create_collection("users", CollectionType::Document);
                    let _friendships = ctx.engine.create_collection("friendships", CollectionType::Edge);
                    println!("✅ Created users and friendships collections");

                    // Add sample users
                    let users = vec![
                        ("alice", json!({"name": "Alice Chen", "age": 28, "city": "Beijing"})),
                        ("bob", json!({"name": "Bob Wang", "age": 32, "city": "Shanghai"})),
                        ("charlie", json!({"name": "Charlie Li", "age": 25, "city": "Shenzhen"})),
                    ];

                    for (key, user_data) in users {
                        let mut data = user_data.as_object().unwrap().clone();
                        data.insert("_key".to_string(), Value::String(key.to_string()));
                        let document = Document::new("users", data)?;
                        ctx.engine.insert_document("users", &document)?;
                        println!("  👤 Added user: {}", key);
                    }

                    // Add friendships
                    let friendships = vec![
                        ("alice", "bob", json!({"since": "2020-01-15", "strength": "close"})),
                        ("alice", "charlie", json!({"since": "2021-03-20", "strength": "good"})),
                        ("bob", "charlie", json!({"since": "2022-01-10", "strength": "good"})),
                    ];

                    for (from_key, to_key, edge_data) in friendships {
                        let mut data = edge_data.as_object().unwrap().clone();
                        data.insert("_from".to_string(), Value::String(format!("users/{}", from_key)));
                        data.insert("_to".to_string(), Value::String(format!("users/{}", to_key)));
                        let document = Document::new("friendships", data)?;
                        ctx.engine.insert_document("friendships", &document)?;
                        println!("  🤝 Added friendship: {} -> {}", from_key, to_key);
                    }

                    println!("🎉 Graph demo completed!");
                    Ok(())
                }

                "setup" if parts.len() >= 3 => {
                    let graph_name = parts[2];
                    let vertex_collection = if parts.len() > 3 { parts[3] } else { "vertices" };
                    let edge_collection = if parts.len() > 4 { parts[4] } else { "edges" };

                    ctx.engine.create_collection(vertex_collection, CollectionType::Document)?;
                    ctx.engine.create_collection(edge_collection, CollectionType::Edge)?;

                    println!("🏗️ Graph '{}' setup completed:", graph_name);
                    println!("  📋 Vertex collection: {}", vertex_collection);
                    println!("  🔗 Edge collection: {}", edge_collection);
                    Ok(())
                }

                "add-vertex" | "vertex" if parts.len() >= 4 => {
                    let collection_name = parts[2];
                    let json_data = parts[3..].join(" ");

                    let data: Map<String, Value> = serde_json::from_str(&json_data)
                        .map_err(|e| ArangoError::bad_parameter(format!("Invalid JSON: {}", e)))?;

                    let document = Document::new(collection_name, data)?;
                    ctx.engine.insert_document(collection_name, &document)?;
                    println!("✅ Vertex added to {} with key: {}", collection_name, document.key());
                    Ok(())
                }

                "add-edge" | "edge" if parts.len() >= 5 => {
                    let collection_name = parts[2];
                    let from_vertex = parts[3];
                    let to_vertex = parts[4];
                    let edge_data = if parts.len() > 5 { parts[5..].join(" ") } else { "{}".to_string() };

                    let mut data: Map<String, Value> = serde_json::from_str(&edge_data)
                        .map_err(|e| ArangoError::bad_parameter(format!("Invalid JSON: {}", e)))?;

                    data.insert("_from".to_string(), Value::String(from_vertex.to_string()));
                    data.insert("_to".to_string(), Value::String(to_vertex.to_string()));

                    let document = Document::new(collection_name, data)?;
                    ctx.engine.insert_document(collection_name, &document)?;
                    println!("✅ Edge added: {} -> {} in {}", from_vertex, to_vertex, collection_name);
                    Ok(())
                }

                _ => {
                    println!("❌ Unknown graph command. Available: demo, setup, add-vertex, add-edge");
                    Ok(())
                }
            }
        }

        "traverse" if parts.len() >= 2 => {
            let start = parts[1];
            let depth: usize = parts.get(2).unwrap_or(&"2").parse().unwrap_or(2);

            println!("🔍 Traversing from vertex '{}' with depth {}:", start, depth);

            // Parse vertex ID
            let vertex_parts: Vec<&str> = start.split('/').collect();
            if vertex_parts.len() == 2 {
                let collection = vertex_parts[0];
                let key = DocumentKey::new(vertex_parts[1])?;

                if let Some(document) = ctx.engine.get_document(collection, &key)? {
                    println!("📍 Start vertex:");
                    println!("{}", serde_json::to_string_pretty(&document.to_json()).unwrap());

                    // 实现完整的图遍历算法
                    let visited_vertices = traverse_graph(&ctx.engine, &start, depth)?;

                    println!("\n🔗 Graph Traversal Results:");
                    println!("   Depth: {}", depth);
                    println!("   Vertices visited: {}", visited_vertices.len());

                    for (level, vertices) in visited_vertices.iter().enumerate() {
                        if !vertices.is_empty() {
                            println!("\n   Level {} ({} vertices):", level, vertices.len());
                            for vertex_info in vertices {
                                println!("     📦 {}", vertex_info.vertex_id);
                                if let Some(ref edge_info) = vertex_info.edge_info {
                                    println!("       🔗 Via edge: {} -> {} ({})",
                                             edge_info.from, edge_info.to, edge_info.edge_data);
                                }
                                if vertex_info.vertex_data.len() > 80 {
                                    println!("       📋 Data: {}...", &vertex_info.vertex_data[..77]);
                                } else {
                                    println!("       📋 Data: {}", vertex_info.vertex_data);
                                }
                            }
                        }
                    }

                    // 统计信息
                    let total_vertices: usize = visited_vertices.iter().map(|v| v.len()).sum();
                    let unique_collections: std::collections::HashSet<_> = visited_vertices
                        .iter()
                        .flat_map(|level| level.iter())
                        .map(|v| v.vertex_id.split('/').next().unwrap_or(""))
                        .collect();

                    println!("\n📊 Traversal Statistics:");
                    println!("   Total vertices found: {}", total_vertices);
                    println!("   Unique collections: {}", unique_collections.len());
                    println!("   Collections involved: {:?}", unique_collections.iter().collect::<Vec<_>>());

                } else {
                    println!("❌ Start vertex not found: {}", start);
                }
            } else {
                println!("❌ Invalid vertex ID format. Use: collection/key");
            }
            Ok(())
        }

        // 数据库操作
        "stats" => {
            let collections = ctx.engine.list_collections();
            let total_collections = collections.len();
            let total_documents: u64 = collections.iter().map(|c| c.document_count).sum();
            let total_size: u64 = collections.iter().map(|c| c.data_size).sum();

            println!("📊 Database Statistics:");
            println!("  Collections: {}", total_collections);
            println!("  Documents: {}", total_documents);
            println!("  Total size: {} bytes", total_size);
            Ok(())
        }

        "compact" => {
            println!("🔧 Database compaction not yet implemented");
            Ok(())
        }

        // 简化命令
        "ls" | "list" => {
            let collections = ctx.engine.list_collections();
            println!("📋 Collections:");
            for collection in collections {
                println!("  - {} ({}) - {} docs",
                         collection.name,
                         collection.collection_type.as_str(),
                         collection.document_count);
            }
            Ok(())
        }

        "clear" => {
            print!("\x1B[2J\x1B[1;1H"); // Clear screen
            Ok(())
        }

        _ => {
            println!("❌ Unknown command: {}", input);
            println!("💡 Type 'help' for available commands");
            Ok(())
        }
    };

    // 计算并显示执行时间
    let duration = start_time.elapsed();
    let duration_ms = duration.as_millis();
    let duration_us = duration.as_micros();

    if duration_ms > 0 {
        println!("⏱️  Executed in {:.2}ms", duration.as_secs_f64() * 1000.0);
    } else if duration_us > 100 {
        println!("⏱️  Executed in {}μs", duration_us);
    } else {
        println!("⏱️  Executed in <100μs");
    }

    result
}

fn print_shell_help() {
    println!("🐚 Mini ArangoDB Interactive Shell Commands");
    println!("==========================================");
    println!();
    println!("📋 Collection Management:");
    println!("  collections, ls, list            - List all collections");
    println!("  create collection <name> [type]  - Create collection (type: document|edge)");
    println!("  drop collection <name>           - Drop a collection");
    println!("  info <collection>                - Show collection information");
    println!();
    println!("📝 Document Operations:");
    println!("  insert <collection> <json>       - Insert a document");
    println!("  get <collection> <key>           - Get document by key");
    println!("  update <collection> <key> <json> - Update document");
    println!("  delete <collection> <key>        - Delete document");
    println!("  count <collection>               - Count documents");
    println!("  show <collection> [limit]        - Show all documents (default limit: 10)");
    println!();
    println!("🔗 Graph Operations:");
    println!("  graph demo                       - Create social network demo");
    println!("  graph setup <name> [vtx] [edge]  - Setup graph collections");
    println!("  graph vertex <collection> <json> - Add vertex");
    println!("  graph edge <collection> <from> <to> [json] - Add edge");
    println!("  traverse <vertex_id> [depth]     - Graph traversal (e.g., users/alice)");
    println!();
    println!("📊 Database Management:");
    println!("  stats                            - Database statistics");
    println!("  compact                          - Compact database");
    println!();
    println!("🛠️ Utility:");
    println!("  help                             - Show this help");
    println!("  clear                            - Clear screen");
    println!("  exit, quit                       - Exit shell");
    println!();
    println!("💡 Examples:");
    println!("  create collection users");
    println!("  insert users {{\"name\": \"Alice\", \"age\": 28}}");
    println!("  graph demo");
    println!("  traverse users/alice 2");
}

async fn handle_collection_command(ctx: DatabaseContext, cmd: CollectionCommand) -> Result<()> {
    match cmd.action {
        CollectionAction::Create { name, collection_type } => {
            let ctype = parse_collection_type(&collection_type)?;
            let collection_info = ctx.engine.create_collection(&name, ctype)?;

            println!("Collection '{}' created successfully", name);
            if ctx.verbose {
                println!("Type: {}", collection_info.collection_type.as_str());
                println!("Created: {}", collection_info.created);
            }
        }
        CollectionAction::Drop { name, force } => {
            if !force {
                print!("Are you sure you want to drop collection '{}'? (y/N): ", name);
                io::stdout().flush().unwrap();

                let mut input = String::new();
                io::stdin().read_line(&mut input).unwrap();

                if input.trim().to_lowercase() != "y" {
                    println!("Operation cancelled");
                    return Ok(());
                }
            }

            // Note: drop_collection not implemented in OptimisticRocksDBEngine yet
            println!("Collection drop not yet implemented");
        }
        CollectionAction::List => {
            let collections = ctx.engine.list_collections();

            if collections.is_empty() {
                println!("No collections found");
            } else {
                println!("Collections:");
                for collection in collections {
                    println!("  {} ({})", collection.name, collection.collection_type.as_str());
                    if ctx.verbose {
                        println!("    Documents: {}", collection.document_count);
                        println!("    Size: {} bytes", collection.data_size);
                        println!("    Created: {}", collection.created);
                    }
                }
            }
        }
        CollectionAction::Info { name } => {
            if let Some(collection) = ctx.engine.get_collection(&name) {
                println!("Collection: {}", collection.name);
                println!("Type: {}", collection.collection_type.as_str());
                println!("Documents: {}", collection.document_count);
                println!("Data size: {} bytes", collection.data_size);
                println!("Created: {}", collection.created);
            } else {
                println!("Collection '{}' not found", name);
            }
        }
    }

    Ok(())
}

async fn handle_document_command(ctx: DatabaseContext, cmd: DocumentCommand) -> Result<()> {
    let collection_name = &cmd.collection;

    // Verify collection exists
    if ctx.engine.get_collection(collection_name).is_none() {
        return Err(ArangoError::collection_not_found(collection_name));
    }

    match cmd.action {
        DocumentAction::Insert { data, key } => {
            let mut doc_data: Map<String, Value> = serde_json::from_str(&data)
                .map_err(|e| ArangoError::bad_parameter(format!("Invalid JSON: {}", e)))?;

            // Add key if provided
            if let Some(k) = key {
                doc_data.insert("_key".to_string(), Value::String(k));
            }

            let document = Document::new(collection_name, doc_data)?;
            ctx.engine.insert_document(collection_name, &document)?;

            println!("Document inserted successfully");
            println!("Key: {}", document.key());
            println!("ID: {}", document.id());
        }
        DocumentAction::Get { key } => {
            let doc_key = DocumentKey::new(key)?;

            if let Some(document) = ctx.engine.get_document(collection_name, &doc_key)? {
                println!("{}", serde_json::to_string_pretty(&document.to_json()).unwrap());
            } else {
                println!("Document not found");
            }
        }
        DocumentAction::Update { key, data } => {
            let doc_key = DocumentKey::new(key)?;

            // Get existing document
            if let Some(mut document) = ctx.engine.get_document(collection_name, &doc_key)? {
                let update_data: Map<String, Value> = serde_json::from_str(&data)
                    .map_err(|e| ArangoError::bad_parameter(format!("Invalid JSON: {}", e)))?;

                document.merge(update_data);
                ctx.engine.update_document(collection_name, &document)?;

                println!("Document updated successfully");
            } else {
                println!("Document not found");
            }
        }
        DocumentAction::Delete { key } => {
            let doc_key = DocumentKey::new(key)?;

            if ctx.engine.delete_document(collection_name, &doc_key)? {
                println!("Document deleted successfully");
            } else {
                println!("Document not found");
            }
        }
        DocumentAction::Find { filter: _, limit: _ } => {
            println!("Find operation not yet fully implemented");
        }
        DocumentAction::Count => {
            // Since count_documents is not implemented in OptimisticRocksDBEngine,
            // we'll show a placeholder
            println!("Document count operation not yet implemented");
        }
    }

    Ok(())
}

async fn handle_database_command(ctx: DatabaseContext, cmd: DatabaseCommand) -> Result<()> {
    match cmd.action {
        DatabaseAction::Stats => {
            let collections = ctx.engine.list_collections();
            let total_collections = collections.len();
            let total_documents: u64 = collections.iter().map(|c| c.document_count).sum();
            let total_size: u64 = collections.iter().map(|c| c.data_size).sum();

            println!("Database Statistics:");
            println!("  Collections: {}", total_collections);
            println!("  Documents: {}", total_documents);
            println!("  Total size: {} bytes", total_size);
        }
        DatabaseAction::Compact => {
            println!("Database compaction not yet implemented");
        }
        DatabaseAction::Init => {
            println!("Database already initialized at startup");
        }
    }

    Ok(())
}

async fn handle_import_command(ctx: DatabaseContext, cmd: ImportCommand) -> Result<()> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    let file = File::open(&cmd.file)
        .map_err(|e| ArangoError::internal(format!("Failed to open file: {}", e)))?;

    let reader = BufReader::new(file);
    let mut count = 0;

    println!("Importing from {} to collection '{}'", cmd.file.display(), cmd.collection);

    match cmd.format.as_str() {
        "jsonl" => {
            for line in reader.lines() {
                let line = line.map_err(|e| ArangoError::internal(format!("Error reading line: {}", e)))?;
                if line.trim().is_empty() {
                    continue;
                }

                let data: Map<String, Value> = serde_json::from_str(&line)
                    .map_err(|e| ArangoError::bad_parameter(format!("Invalid JSON on line {}: {}", count + 1, e)))?;

                let document = Document::new(&cmd.collection, data)?;
                ctx.engine.insert_document(&cmd.collection, &document)?;
                count += 1;

                if count % 1000 == 0 {
                    println!("Imported {} documents...", count);
                }
            }
        }
        "json" => {
            let mut content = String::new();
            for line in reader.lines() {
                content.push_str(&line.unwrap());
            }

            let json_array: Vec<Value> = serde_json::from_str(&content)
                .map_err(|e| ArangoError::bad_parameter(format!("Invalid JSON array: {}", e)))?;

            for item in json_array {
                if let Value::Object(data) = item {
                    let document = Document::new(&cmd.collection, data)?;
                    ctx.engine.insert_document(&cmd.collection, &document)?;
                    count += 1;

                    if count % 1000 == 0 {
                        println!("Imported {} documents...", count);
                    }
                } else {
                    return Err(ArangoError::bad_parameter("Expected JSON object"));
                }
            }
        }
        _ => {
            return Err(ArangoError::bad_parameter(format!("Unsupported format: {}", cmd.format)));
        }
    }

    println!("Import completed. {} documents imported.", count);
    Ok(())
}

async fn handle_server_command(_ctx: DatabaseContext, cmd: ServerCommand) -> Result<()> {
    match cmd.action {
        ServerAction::Start { port, bind } => {
            println!("Starting server on {}:{}", bind, port);
            println!("Note: HTTP server not yet implemented in this CLI version");
            println!("Use the shell mode for interactive database operations");
        }
        ServerAction::Stop => {
            println!("Server stop command received");
            println!("Note: Server management not yet implemented");
        }
        ServerAction::Status => {
            println!("Server status: CLI mode (no HTTP server running)");
        }
    }

    Ok(())
}

async fn handle_graph_command(ctx: DatabaseContext, cmd: GraphCommand) -> Result<()> {
    match cmd.action {
        GraphAction::Demo => {
            println!("🔗 Creating Social Network Graph Demo");
            println!("====================================");

            // Create vertex collection for users
            let users_collection = ctx.engine.create_collection("users", CollectionType::Document)?;
            println!("✅ Created users collection (vertices)");

            // Create edge collection for friendships
            let friendships_collection = ctx.engine.create_collection("friendships", CollectionType::Edge)?;
            println!("✅ Created friendships collection (edges)");

            // Insert sample users (vertices)
            let users = vec![
                json!({
                    "_key": "alice",
                    "name": "Alice Chen",
                    "age": 28,
                    "city": "Beijing",
                    "interests": ["music", "programming"]
                }),
                json!({
                    "_key": "bob", 
                    "name": "Bob Wang",
                    "age": 32,
                    "city": "Shanghai", 
                    "interests": ["sports", "travel"]
                }),
                json!({
                    "_key": "charlie",
                    "name": "Charlie Li",
                    "age": 25,
                    "city": "Shenzhen",
                    "interests": ["gaming", "music"]
                }),
                json!({
                    "_key": "diana",
                    "name": "Diana Zhang",
                    "age": 30,
                    "city": "Guangzhou",
                    "interests": ["art", "travel"]
                }),
            ];

            for user_data in users {
                let data = user_data.as_object().unwrap().clone();
                let document = Document::new("users", data)?;
                ctx.engine.insert_document("users", &document)?;
                println!("  👤 Added user: {}", document.key());
            }

            // Insert friendships (edges)
            let friendships = vec![
                ("alice", "bob", json!({"since": "2020-01-15", "strength": "close"})),
                ("alice", "charlie", json!({"since": "2021-03-20", "strength": "good"})),
                ("bob", "charlie", json!({"since": "2022-01-10", "strength": "good"})),
            ];

            for (from_key, to_key, edge_data) in friendships {
                let mut data = edge_data.as_object().unwrap().clone();
                data.insert("_from".to_string(), Value::String(format!("users/{}", from_key)));
                data.insert("_to".to_string(), Value::String(format!("users/{}", to_key)));

                let document = Document::new("friendships", data)?;
                ctx.engine.insert_document("friendships", &document)?;
                println!("  🤝 Added friendship: {} -> {}", from_key, to_key);
            }

            println!("\n📊 Graph Statistics:");
            println!("  Vertices (Users): 4");
            println!("  Edges (Friendships): 5");

            println!("\n🔍 Try these commands:");
            println!("  ./target/release/arangod graph traverse users/alice");
            println!("  ./target/release/arangod document --collection users get alice");
            println!("  ./target/release/arangod document --collection friendships find");
        }

        GraphAction::Setup { name, vertex_collection, edge_collection } => {
            println!("Setting up graph '{}' with:", name);

            // Create vertex collection
            ctx.engine.create_collection(&vertex_collection, CollectionType::Document)?;
            println!("✅ Created vertex collection: {}", vertex_collection);

            // Create edge collection
            ctx.engine.create_collection(&edge_collection, CollectionType::Edge)?;
            println!("✅ Created edge collection: {}", edge_collection);

            println!("Graph setup completed! You can now:");
            println!("  - Insert vertices: arangod graph insert-vertex {} '{{...}}'", vertex_collection);
            println!("  - Insert edges: arangod graph insert-edge {} from/key to/key", edge_collection);
        }

        GraphAction::InsertVertex { collection, data, key } => {
            let mut doc_data: Map<String, Value> = serde_json::from_str(&data)
                .map_err(|e| ArangoError::bad_parameter(format!("Invalid JSON: {}", e)))?;

            if let Some(k) = key {
                doc_data.insert("_key".to_string(), Value::String(k));
            }

            let document = Document::new(&collection, doc_data)?;
            ctx.engine.insert_document(&collection, &document)?;

            println!("✅ Inserted vertex: {}/{}", collection, document.key());
        }

        GraphAction::InsertEdge { collection, from, to, data } => {
            let mut edge_data: Map<String, Value> = serde_json::from_str(&data)
                .map_err(|e| ArangoError::bad_parameter(format!("Invalid JSON: {}", e)))?;

            edge_data.insert("_from".to_string(), Value::String(from.clone()));
            edge_data.insert("_to".to_string(), Value::String(to.clone()));

            let document = Document::new(&collection, edge_data)?;
            ctx.engine.insert_document(&collection, &document)?;

            println!("✅ Inserted edge: {} -> {} in collection {}", from, to, collection);
        }

        GraphAction::Traverse { start, depth } => {
            println!("🔍 Traversing from vertex: {}", start);
            println!("Max depth: {}", depth);

            // Parse start vertex
            let parts: Vec<&str> = start.split('/').collect();
            if parts.len() != 2 {
                return Err(ArangoError::bad_parameter("Vertex ID must be in format 'collection/key'"));
            }

            let collection = parts[0];
            let key = DocumentKey::new(parts[1])?;

            // Get start vertex
            if let Some(start_vertex) = ctx.engine.get_document(collection, &key)? {
                println!("\n📍 Start Vertex:");
                println!("{}", serde_json::to_string_pretty(&start_vertex.to_json()).unwrap());

                // 实现完整的图遍历算法
                let visited_vertices = traverse_graph(&ctx.engine, &start, depth)?;

                println!("\n🔗 Graph Traversal Results:");
                println!("   Depth: {}", depth);
                println!("   Vertices visited: {}", visited_vertices.len());

                for (level, vertices) in visited_vertices.iter().enumerate() {
                    if !vertices.is_empty() {
                        println!("\n   Level {} ({} vertices):", level, vertices.len());
                        for vertex_info in vertices {
                            println!("     📦 {}", vertex_info.vertex_id);
                            if let Some(ref edge_info) = vertex_info.edge_info {
                                println!("       🔗 Via edge: {} -> {} ({})",
                                         edge_info.from, edge_info.to, edge_info.edge_data);
                            }
                            if vertex_info.vertex_data.len() > 80 {
                                println!("       📋 Data: {}...", &vertex_info.vertex_data[..77]);
                            } else {
                                println!("       📋 Data: {}", vertex_info.vertex_data);
                            }
                        }
                    }
                }

                // 统计信息
                let total_vertices: usize = visited_vertices.iter().map(|v| v.len()).sum();
                let unique_collections: std::collections::HashSet<_> = visited_vertices
                    .iter()
                    .flat_map(|level| level.iter())
                    .map(|v| v.vertex_id.split('/').next().unwrap_or(""))
                    .collect();

                println!("\n📊 Traversal Statistics:");
                println!("   Total vertices found: {}", total_vertices);
                println!("   Unique collections: {}", unique_collections.len());
                println!("   Collections involved: {:?}", unique_collections.iter().collect::<Vec<_>>());

            } else {
                println!("❌ Start vertex not found: {}", start);
            }
        }
    }

    Ok(())
}

fn parse_collection_type(type_str: &str) -> Result<CollectionType> {
    match type_str.to_lowercase().as_str() {
        "document" | "doc" => Ok(CollectionType::Document),
        "edge" => Ok(CollectionType::Edge),
        _ => Err(ArangoError::bad_parameter(format!("Invalid collection type: {}", type_str))),
    }
} 