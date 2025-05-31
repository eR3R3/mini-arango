use std::fmt;
use thiserror::Error;

/// ArangoDB error codes - mirrored from the C++ implementation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ErrorCode {
    // General errors
    NoError = 0,
    Failed = 1,
    SystemError = 2,
    OutOfMemory = 3,
    Internal = 4,
    IllegalNumber = 5,
    NumericOverflow = 6,
    IllegalOption = 7,
    DeadPid = 8,
    NotImplemented = 9,
    BadParameter = 10,
    Forbidden = 11,
    OutOfMemoryMmap = 12,
    Corrupted = 13,
    FileNotFound = 14,
    CannotWriteFile = 15,
    CannotOverwriteFile = 16,
    TypeErrorName = 17,
    LockTimeout = 18,
    CannotCreateDirectory = 19,
    CannotCreateTempFile = 20,
    RequestCanceled = 21,
    DebugState = 22,
    IpAddressInvalid = 25,
    FileExists = 27,
    Locked = 28,
    Deadlock = 29,
    ShuttingDown = 30,
    OnlyEnterprise = 31,
    ResourceLimit = 32,
    InvalidFormatVersion = 33,
    GotSignal = 35,
    ProcessingError = 37,
    SerializationError = 40,

    // HTTP errors
    HttpBadParameter = 400,
    HttpUnauthorized = 401,
    HttpForbidden = 403,
    HttpNotFound = 404,
    HttpMethodNotSupported = 405,
    HttpNotAcceptable = 406,
    HttpPreconditionFailed = 412,
    HttpRequestEntityTooLarge = 413,
    HttpRequestUriTooLong = 414,
    HttpInternalServerError = 500,
    HttpNotImplemented = 501,
    HttpBadGateway = 502,
    HttpServiceUnavailable = 503,

    // ArangoDB errors
    ArangoInvalidDatabase = 1200,
    ArangoUseSystemDatabase = 1201,
    ArangoDocumentNotFound = 1202,
    ArangoDataSourceNotFound = 1203,
    ArangoCollectionParameterMissing = 1204,
    ArangoDocumentHandleBad = 1205,
    ArangoMaxWaitTimeout = 1206,
    ArangoDocumentKeyBad = 1208,
    ArangoDocumentKeyUnexpected = 1209,
    ArangoDataDir = 1210,
    ArangoMmapFailed = 1211,
    ArangoFilesystemFull = 1212,
    ArangoNoJournal = 1213,
    ArangoDatafileAlreadyExists = 1214,
    ArangoDatafileUnreadable = 1215,
    ArangoDatafileEmpty = 1216,
    ArangoRecovery = 1217,
    ArangoDatafileStatistics = 1218,
    ArangoCorrupted = 1221,
    ArangoCollectionUnloaded = 1223,
    ArangoCollectionTypeMismatch = 1224,
    ArangoCollectionNotUnloaded = 1225,
    ArangoCorruptedCollection = 1226,
    ArangoReadOnly = 1230,
    ArangoDuplicateName = 1232,
    ArangoDuplicateIdentifier = 1233,
    ArangoDatafileUnsupportedVersion = 1235,
    ArangoCollectionKeysMissing = 1237,
    ArangoInvalidCollection = 1238,
    ArangoCollectionTypeInvalid = 1239,
    ArangoConflict = 1240,
    ArangoUniqueConstraintViolated = 1241,
    ArangoIndexNotFound = 1242,
    ArangoIndexTypeInvalid = 1243,
    ArangoDocumentTooLarge = 1244,
    ArangoCrossCollectionRequest = 1245,
    ArangoIndexHandleBad = 1246,
    ArangoDocumentRevBad = 1247,
    ArangoDocumentKeyMissing = 1248,
    ArangoIncompleteRead = 1249,

    // Transaction errors
    TransactionUnregisteredCollection = 1900,
    TransactionDisallowedOperation = 1901,
    TransactionAborted = 1902,
    TransactionNotFound = 1903,
    TransactionInternal = 1904,
    TransactionNested = 1905,
    TransactionConflict = 1906,

    // Query errors
    QueryKilled = 1500,
    QueryInvalidSyntax = 1501,
    QueryEmpty = 1502,
    QueryScript = 1503,
    QueryNumberOutOfRange = 1504,
    QueryVariableNameInvalid = 1510,
    QueryVariableNameUnknown = 1511,
    QueryCollectionLockFailed = 1521,
    QueryTooManyCollections = 1522,
    QueryDocumentAttributeRedeclared = 1530,
    QueryFunctionNameUnknown = 1540,
    QueryFunctionArgumentNumberMismatch = 1541,
    QueryFunctionArgumentTypeMismatch = 1542,
    QueryInvalidRegex = 1543,
    QueryBindParametersInvalid = 1550,
    QueryBindParameterMissing = 1551,
    QueryBindParameterUndeclared = 1552,
    QueryBindParameterType = 1553,
    QueryInvalidLogicalValue = 1560,
    QueryInvalidArithmeticValue = 1561,
    QueryDivisionByZero = 1562,
    QueryArrayExpected = 1563,
    QueryFailCalled = 1569,
    QueryGeoIndexMissing = 1570,
    QueryFulltextIndexMissing = 1571,
    QueryInvalidDateValue = 1572,
    QueryMultiModify = 1573,
    QueryInvalidAggregateExpression = 1574,
    QueryCompileTimeUnknown = 1575,
    QueryException = 1576,
    QueryInUse = 1577,
    QueryBadJsonPlan = 1578,
    QueryNotFound = 1579,
    QueryInternalError = 1580,

    // Graph errors
    GraphInvalidGraph = 1920,
    GraphCouldNotCreateGraph = 1921,
    GraphInvalidVertex = 1922,
    GraphCouldNotCreateVertex = 1923,
    GraphCouldNotChangeVertex = 1924,
    GraphInvalidEdge = 1925,
    GraphCouldNotCreateEdge = 1926,
    GraphCouldNotChangeEdge = 1927,
    GraphTooManyIterations = 1928,
    GraphInvalidFilterResult = 1929,
    GraphCollectionMultiUse = 1930,
    GraphCollectionUseInMultiGraphs = 1931,
    GraphCreateMalformedEdgeDefinition = 1932,
    GraphCreateMissingName = 1933,
    GraphNotFound = 1934,
    GraphDuplicateError = 1935,
    GraphVertexColDoesNotExist = 1936,
    GraphWrongCollectionTypeVertex = 1937,
    GraphNotInOrphanCollection = 1938,
    GraphCollectionUsedInEdgeDef = 1939,
    GraphEdgeCollectionNotUsed = 1940,
    GraphNotAnArangoCollection = 1941,
    GraphNoGraphCollection = 1942,
    GraphInvalidParameterError = 1943,
    GraphInvalidIdError = 1944,
    GraphEmptyError = 1945,
    GraphInternalDataError = 1946,
    GraphCreateMalformedOrphanList = 1947,
    GraphEdgeDefinitionIsUndirected = 1948,
    GraphCollectionUsedInOrphans = 1949,
    GraphEdgeColDoesNotExist = 1950,
    GraphEmpty = 1951,
}

impl ErrorCode {
    pub fn as_u32(&self) -> u32 {
        *self as u32
    }

    pub fn from_u32(value: u32) -> Self {
        // This is safe because we're using repr(u32) and handling unknown values
        match value {
            0 => ErrorCode::NoError,
            1 => ErrorCode::Failed,
            2 => ErrorCode::SystemError,
            3 => ErrorCode::OutOfMemory,
            4 => ErrorCode::Internal,
            400 => ErrorCode::HttpBadParameter,
            401 => ErrorCode::HttpUnauthorized,
            403 => ErrorCode::HttpForbidden,
            404 => ErrorCode::HttpNotFound,
            1200 => ErrorCode::ArangoInvalidDatabase,
            1201 => ErrorCode::ArangoUseSystemDatabase,
            1202 => ErrorCode::ArangoDocumentNotFound,
            1203 => ErrorCode::ArangoDataSourceNotFound,
            1241 => ErrorCode::ArangoUniqueConstraintViolated,
            1239 => ErrorCode::ArangoCollectionTypeInvalid,
            1500 => ErrorCode::QueryKilled,
            1906 => ErrorCode::TransactionConflict,
            1920 => ErrorCode::GraphInvalidGraph,
            1934 => ErrorCode::GraphNotFound,
            _ => ErrorCode::Internal, // Unknown error codes default to Internal
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCode::NoError => write!(f, "no error"),
            ErrorCode::Failed => write!(f, "failed"),
            ErrorCode::SystemError => write!(f, "system error"),
            ErrorCode::OutOfMemory => write!(f, "out of memory"),
            ErrorCode::Internal => write!(f, "internal error"),
            ErrorCode::BadParameter => write!(f, "bad parameter"),
            ErrorCode::Forbidden => write!(f, "forbidden"),
            ErrorCode::HttpBadParameter => write!(f, "bad parameter"),
            ErrorCode::HttpUnauthorized => write!(f, "unauthorized"),
            ErrorCode::HttpForbidden => write!(f, "forbidden"),
            ErrorCode::HttpNotFound => write!(f, "not found"),
            ErrorCode::ArangoInvalidDatabase => write!(f, "invalid database"),
            ErrorCode::ArangoDocumentNotFound => write!(f, "document not found"),
            ErrorCode::ArangoDataSourceNotFound => write!(f, "collection or view not found"),
            ErrorCode::ArangoUniqueConstraintViolated => write!(f, "unique constraint violated"),
            ErrorCode::ArangoCollectionTypeInvalid => write!(f, "invalid collection type"),
            ErrorCode::QueryKilled => write!(f, "query killed"),
            ErrorCode::TransactionConflict => write!(f, "transaction conflict"),
            ErrorCode::GraphInvalidGraph => write!(f, "invalid graph"),
            ErrorCode::GraphNotFound => write!(f, "graph not found"),
            _ => write!(f, "error code {}", self.as_u32()),
        }
    }
}

/// Main ArangoDB error type
#[derive(Error, Debug)]
pub enum ArangoError {
    #[error("ArangoDB error {code}: {message}")]
    Database {
        code: ErrorCode,
        message: String,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("UUID error: {0}")]
    Uuid(#[from] uuid::Error),

    #[error("HTTP error: {0}")]
    Http(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Graph error: {0}")]
    Graph(String),

    #[error("Index error: {0}")]
    Index(String),
}

impl ArangoError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        ArangoError::Database {
            code,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        ArangoError::new(ErrorCode::Internal, message)
    }

    pub fn bad_parameter(message: impl Into<String>) -> Self {
        ArangoError::new(ErrorCode::BadParameter, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        ArangoError::new(ErrorCode::ArangoDocumentNotFound, message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        ArangoError::new(ErrorCode::Forbidden, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        ArangoError::new(ErrorCode::ArangoConflict, message)
    }

    pub fn unique_constraint_violated(message: impl Into<String>) -> Self {
        ArangoError::new(ErrorCode::ArangoUniqueConstraintViolated, message)
    }

    pub fn collection_not_found(collection_name: impl Into<String>) -> Self {
        ArangoError::new(
            ErrorCode::ArangoDataSourceNotFound,
            format!("collection '{}' not found", collection_name.into())
        )
    }

    pub fn document_not_found(key: impl Into<String>) -> Self {
        ArangoError::new(
            ErrorCode::ArangoDocumentNotFound,
            format!("document '{}' not found", key.into())
        )
    }

    pub fn graph_not_found(graph_name: impl Into<String>) -> Self {
        ArangoError::new(
            ErrorCode::GraphNotFound,
            format!("graph '{}' not found", graph_name.into())
        )
    }

    pub fn transaction_conflict(message: impl Into<String>) -> Self {
        ArangoError::new(ErrorCode::TransactionConflict, message)
    }

    pub fn invalid_index_definition(message: impl Into<String>) -> Self {
        ArangoError::new(ErrorCode::ArangoIndexTypeInvalid, message)
    }

    pub fn invalid_index_data(message: impl Into<String>) -> Self {
        ArangoError::new(ErrorCode::ArangoCorrupted, message)
    }

    pub fn error_code(&self) -> ErrorCode {
        match self {
            ArangoError::Database { code, .. } => *code,
            ArangoError::Io(_) => ErrorCode::SystemError,
            ArangoError::RocksDb(_) => ErrorCode::Internal,
            ArangoError::Serde(_) => ErrorCode::SerializationError,
            ArangoError::Utf8(_) => ErrorCode::BadParameter,
            ArangoError::Uuid(_) => ErrorCode::BadParameter,
            ArangoError::Http(_) => ErrorCode::HttpInternalServerError,
            ArangoError::Validation(_) => ErrorCode::BadParameter,
            ArangoError::Parse(_) => ErrorCode::BadParameter,
            ArangoError::Config(_) => ErrorCode::BadParameter,
            ArangoError::Network(_) => ErrorCode::HttpInternalServerError,
            ArangoError::Transaction(_) => ErrorCode::TransactionInternal,
            ArangoError::Query(_) => ErrorCode::QueryInternalError,
            ArangoError::Graph(_) => ErrorCode::GraphInvalidGraph,
            ArangoError::Index(_) => ErrorCode::ArangoIndexNotFound,
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self.error_code(), 
            ErrorCode::ArangoDocumentNotFound | 
            ErrorCode::ArangoDataSourceNotFound |
            ErrorCode::HttpNotFound |
            ErrorCode::GraphNotFound
        )
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self.error_code(), 
            ErrorCode::ArangoConflict | 
            ErrorCode::ArangoUniqueConstraintViolated |
            ErrorCode::ArangoDuplicateName
        )
    }
}

/// Result type alias for ArangoDB operations
pub type Result<T> = std::result::Result<T, ArangoError>;

/// Macro for creating ArangoDB errors
#[macro_export]
macro_rules! arango_error {
    ($code:expr, $($arg:tt)*) => {
        $crate::common::error::ArangoError::new($code, format!($($arg)*))
    };
}

/// Macro for early return on error
#[macro_export]
macro_rules! arango_bail {
    ($code:expr, $($arg:tt)*) => {
        return Err(arango_error!($code, $($arg)*))
    };
}

/// Macro for ensuring a condition or returning an error
#[macro_export]
macro_rules! arango_ensure {
    ($cond:expr, $code:expr, $($arg:tt)*) => {
        if !($cond) {
            arango_bail!($code, $($arg)*);
        }
    };
} 