// This file contains your Data Connector logic
section Clickhouse;

Config_DriverName = "ClickHouse ODBC Driver (Unicode)";

EnableTraceOutput = true;

// 
// Load common library functions
// 
Extension.LoadFunction = (name as text) =>
    let
        binary = Extension.Contents(name),
        asText = Text.FromBinary(binary)
    in
        Expression.Evaluate(asText, #shared);


// Diagnostics module contains multiple functions. We can take the ones we need.
Diagnostics = Extension.LoadFunction("Diagnostics.pqm");
Diagnostics.LogValue = if (EnableTraceOutput) then Diagnostics[LogValue] else (prefix, value) => value;

// OdbcConstants contains numeric constants from the ODBC header files, and a 
// helper function to create bitfield values.
ODBC = Extension.LoadFunction("OdbcConstants.pqm");
Odbc.Flags = ODBC[Flags];


Config_SqlConformance = ODBC[SQL_SC][SQL_SC_SQL92_FULL];  // null, 1, 2, 4, 8

ImplicitTypeConversions = #table(
    { "Type1",        "Type2",         "ResultType" }, {
    // 'bpchar' char is added here to allow it to be converted to 'char' when compared against constants.
    { "Int64",       "BIGINT",          "BIGINT" },
    { "UInt32",       "BIGINT",          "BIGINT" },
    { "UInt16",       "BIGINT",          "BIGINT" },
    { "UInt8",       "BIGINT",          "BIGINT" }
});

[DataSource.Kind="Clickhouse", Publish="Clickhouse.Publish"]
shared Clickhouse.Database =
    Value.ReplaceType(
        ClickhouseImpl,
        ClickhouseType
    );

    
//[DataSource.Kind="Clickhouse", Publish="Clickhouse.Publish"]
//shared Clickhouse.Database = Value.NativeQuery(ClickhouseImpl, query);



//handling the UI part of the connector requires using (custom) types and metadata, see the links below for more information:
//https://docs.microsoft.com/en-us/powerquery-m/m-spec-types
//https://bengribaudo.com/blog/2021/03/17/5523/power-query-m-primer-part20-metadata
//https://docs.microsoft.com/en-us/power-query/handlingdocumentation
//the implementation part itself is documented in the data connectors odbc sample (sqlODBC:
//but for some reason no-one includes their UI definition.
//recOptions = type [ Name = text, Age = number ]
//UI ( https://docs.microsoft.com/en-us/power-query/handlingdocumentation )
ClickhouseType =
    type function (server as
        (
            type text
            meta
            [
                Documentation.FieldCaption = "Connection String",
                Documentation.FieldDescription = "ClickHouse Connection String, e.g. http://localhost:8123",
                Documentation.SampleValues = {
                    "http://localhost:8123"
                }
            ]
        ), optional database as
        (
            type text
            meta
            [
                Documentation.FieldCaption = "Database",
                Documentation.FieldDescription = "database name to be connected",
                Documentation.DefaultValue = {
                    "default"
                }
            ]
        ),
        optional query as
        (
            type text
            meta
            [
                Documentation.FieldCaption = "Query",
                //Extension.LoadString("Parameter.Query.Caption"),
                Documentation.FieldDescription = "You can enter a custom query here.",
                Documentation.SampleValues = {
                    "SELECT * FROM numbers(10) ;"
                },
                Formatting.IsMultiLine = true,
                Formatting.IsCode = true
            ]
        )) as table
    meta
    [
        Documentation.Name = "Clickhouse",
        Documentation.LongDescription = "Clickhouse",
        Documentation.Icon = Extension.Contents("Clickhouse32.png")
    ];


ClickhouseImpl = (server as text, optional database as text, optional query as text) =>
    let
        // This record contains all of the connection string properties we 
        // will set for this ODBC driver. The 'Driver' field is required for 
        // all ODBC connections. Other properties will vary between ODBC drivers,
        // but generally take Server and Database properties. Note that
        // credential related properties will be set separately.


        // Get the current credential, and check what type of authentication we're using
        Credential = Extension.CurrentCredential(),
        // Credentials are passed to the ODBC driver using the CredentialConnectionString field.
        // If the user has selected SQL auth (i.e. UsernamePassword), we'll set the 
        // UID and PWD connection string properties. This should be standard across ODBC drivers.
        // If the user has selected Windows auth, we'll set the Trusted_Connection property. 
        // Trusted_Connection is specific to the SQL Server Native Client ODBC driver.
        // Other drivers might require additional connection string properties to be set.
        CredentialConnectionString =
            if (Credential[AuthenticationKind]?) = "UsernamePassword" then 
                [ UID = Credential[Username], PWD = Credential[Password] ]
            // unknown authentication kind - return an 'unimplemented' error
            else
                ..., 
        EncryptConnection = Credential[EncryptConnection]?,

        SSLMode =
            if EncryptConnection = null or EncryptConnection = true then
                "require"
            else
                "prefer",

        splitServerList = Text.Split(server, "="),


        dsnNamePart = splitServerList{1},
            dsnName =
                Text.Remove(
                    dsnNamePart,
                    {
                        "'",
                        """"
                    }
                ),

        BaseConnectionString =
        [
            Driver = Config_DriverName,
            Url = server,
            HugeIntAsString = "on",
            VerifyConnectionEarly = "on",
            SSLMode = SSLMode
        ],

        is_query =
            if (query <> null) then
                 true
            else
                false,

        is_database =
            if (database <> null) then
                 true
            else
                false,
         


        //ConnectionString = AddConnectionStringOption(BaseConnectionString, "prefer_column_name_to_alias", 1),


        ConnectionString= BaseConnectionString,

        // Here our connector is wrapping M's Odbc.DataSource() function. 
        //
        // The first argument will be the connection string. It can be passed in as a record,
        // or an actual text value. When using a record, M will ensure that the values will be 
        // property encoded. 
        // 
        // The second argument is the options record which allows us to set the credential
        // connection string properties, and override default behaviors.  
        OdbcDataSource = Odbc.DataSource(ConnectionString, [
            // Pass the credential-specific part of the connection string
            CredentialConnectionString = CredentialConnectionString,
            // Enables client side connection pooling for the ODBC driver.
            // Most drivers will want to set this value to true.
            ClientConnectionPooling = true,
            // When HierarchialNavigation is set to true, the navigation tree
            // will be organized by Database -> Schema -> Table. When set to false,
            // all tables will be displayed in a flat list using fully qualified names. 
            HierarchicalNavigation = true,
            //AstVisitor = AstVisitor,
            
            //ImplicitTypeConversions = ImplicitTypeConversions,

            CreateNavigationProperties = false,
            // Use the SqlCapabilities record to specify driver capabilities that are not
            // discoverable through ODBC 3.8, and to override capabilities reported by
            // the driver. 
            SqlCapabilities = [
                Sql92Conformance = Config_SqlConformance,
                GroupByCapabilities = ODBC[SQL_GB][SQL_GB_GROUP_BY_CONTAINS_SELECT] /* SQL_GB_GROUP_BY_CONTAINS_SELECT */,
                FractionalSecondsScale = 3,
                SupportsNumericLiterals = true,
                SupportsStringLiterals = true,
                SupportsOdbcDateLiterals = true,
                SupportsOdbcTimeLiterals = true,
                SupportsOdbcTimestampLiterals = true,
                LimitClauseKind = LimitClauseKind.Top
            ],
            SoftNumbers = true,
            HideNativeQuery = false,
            // Use the SQLGetInfo record to override values returned by the driver.
            SQLGetInfo = [
                SQL_SQL92_PREDICATES = 0x0000FFFF, /* */
                SQL_AGGREGATE_FUNCTIONS = 0xFF,

                SQL_CONVERT_FUNCTIONS = 0x00000002, //  Tell Power BI that Exasol only knows Casts so no CONVERT functions are generated
                SQL_CONVERT_VARCHAR = 0x0082F1FF,   // Tell Power BI that Exasol also is able to convert SQL_WVARCHAR, additional fix for Unicode characters (Exasol ODBC returns 0x0002F1FF)
                SQL_CONVERT_CHAR = 0x0022F1FF ,   // Tell Power BI that Exasol also is able to convert SQL_WCHAR, additional fix for Unicode characters (Exasol ODBC returns 0x0002F1FF)
                SQL_CONVERT_WVARCHAR = 0x0082F1FF,   // Tell Power BI that Exasol also is able to convert SQL_WVARCHAR, additional fix for Unicode characters (Exasol ODBC returns 0x0002F1FF)
                SQL_CONVERT_WCHAR = 0x0022F1FF    // Tell Power BI that Exasol also is able to convert SQL_WCHAR, additional fix for Unicode characters (Exasol ODBC returns 0x0002F1FF)

            ],
            SQLGetFunctions = [
            ],
            SQLGetTypeInfo = Odbc.DataSource.Options.SQLGetTypeInfo,
            SQLColumns = Odbc.DataSource.Options.SQLColumns
        ]),
        // The first level of the navigation table will be the name of the database the user
        // passed in. Rather than repeating it again, we'll select it ({[Name = database]}) 
        // and access the next level of the navigation table.
        Database = OdbcDataSource{[Name = database]}[Data],

        odbcQueryResult =
                Odbc.Query(
                        ConnectionString,
                        query,
                        [CredentialConnectionString = CredentialConnectionString]
                    ),
        result =
                if is_query = false then
                    if is_database = true then
                        OdbcDataSource{[Name = database]}[Data]
                    else 
                        OdbcDataSource
                else
                    odbcQueryResult
    in
        result;

Odbc.DataSource.Options.SQLGetTypeInfo =
    (types as table) as table =>
        let
            newTypes =
                #table(
                    {
                        "TYPE_NAME",
                        "DATA_TYPE",
                        "COLUMN_SIZE",
                        "LITERAL_PREFIX",
                        "LITERAL_SUFFIX",
                        "CREATE_PARAMS",
                        "NULLABLE",
                        "CASE_SENSITIVE",
                        "SEARCHABLE",
                        "UNSIGNED_ATTRIBUTE",
                        "FIXED_PREC_SCALE",
                        "AUTO_UNIQUE_VALUE",
                        "LOCAL_TYPE_NAME",
                        "MINIMUM_SCALE",
                        "MAXIMUM_SCALE",
                        "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB",
                        "NUM_PREC_RADIX",
                        "INTERVAL_PRECISION"
                    },
                    // we add a new entry for each type we want to add, the following entries are needed so that Power BI is able to handle Unicode characters
                    {
 			{
                                "SQL_WCHAR", -8, 2000, "'", "'", "max length", 1, 1, 3, null, 0, null, "SQL_WCHAR", null, null, -8, null, null, null
                            },
                            {
                                "SQL_WVARCHAR", -9, 2000000, "'", "'", "max length", 1, 1, 3, null, 0, null, "SQL_WVARCHAR", null, null, -9, null, null, null
                            },
                        {
                            "SQL_BIGINT",
                            -5,
                            20,
                            "",
                            "",
                            "",
                            0,
                            1,
                            3,
                            1,
                            0,
                            0,
                            "UInt64",
                            0,
                            0,
                            -5,
                            0,
                            10,
                            0
                        },{
                            "SQL_BIGINT",
                            -5,
                            20,
                            "",
                            "",
                            "",
                            0,
                            1,
                            3,
                            0,
                            0,
                            0,
                            "Int64",
                            0,
                            0,
                            -5,
                            0,
                            10,
                            0
                        }
                    }
                ),
            //the new types get added to the exising ones and the merged table gets returned
            append =
                Table.Combine(
                    {
                        types,
                        newTypes
                    }
                )
        in
            append;

Odbc.DataSource.Options.SQLColumns =
         (catalogName, schemaName, tableName, columnName, source) =>
                let
                    OdbcSqlType.DATETIME = 9,
                    OdbcSqlType.TEXT = 12,
                    OdbcSqlType.WVARCHAR = -9,
                    OdbcSqlType.TYPE_DATE = 91,
                    OdbcSqlType.TIME = 10,
                    OdbcSqlType.TINYINT = 250,
                    OdbcSqlType.TINYINT_TRUE = -6,
                    OdbcSqlType.SMALLINT = 5,
                    OdbcSqlType.BIGINT = 251,
                    OdbcSqlType.DOUBLE = 8,
                    OdbcSqlType.INT = 4,
                    OdbcSqlType.TYPE_TIME = 92,
                    OdbcSqlType.TIMESTAMP = 11,
                    OdbcSqlType.TYPE_TIMESTAMP = 93,
                    OdbcSqlType.BIGINT_TRUE = -5,
                    OdbcSqlType.GUID_TRUE = -11,
                    OdbcSqlType.GUID = 245,


                    FixNullable = (nullable) => 
                        if nullable = "0" then "YES"
                        else "NO",


                    FixDataType = (dataType) =>
                        if dataType = OdbcSqlType.DATETIME then
                            OdbcSqlType.TYPE_TIMESTAMP
                        else if dataType = OdbcSqlType.TIME then
                            OdbcSqlType.TYPE_TIME
                        else if dataType = OdbcSqlType.TIMESTAMP then
                            OdbcSqlType.TYPE_TIMESTAMP
                        else if dataType = OdbcSqlType.TINYINT then
                            OdbcSqlType.TINYINT_TRUE
                        else if dataType = OdbcSqlType.BIGINT then
                            OdbcSqlType.BIGINT_TRUE
                        else if dataType = OdbcSqlType.TEXT then
                            OdbcSqlType.WVARCHAR
                        else if dataType = OdbcSqlType.GUID then
                            OdbcSqlType.GUID_TRUE
                        else
                            dataType,
                    FixDataTypeName = (dataTypeName) =>
                        if dataTypeName = "TEXT" then
                            "SQL_WVARCHAR"
                        else if dataTypeName = "CHAR" then
                            "SQL_WCHAR"
                        else
                            dataTypeName,
                    Transform2 = Table.TransformColumns(source, { { "DATA_TYPE", FixDataType } , { "TYPE_NAME", FixDataTypeName }}),
                    Transform3 = Table.TransformColumnTypes(Transform2, {{"IS_NULLABLE", Text.Type}}),
                    Transform1 = Table.TransformColumns(Transform3, {{ "IS_NULLABLE", Number.FromText} }),
                    Transform = Table.TransformColumns(Transform1, {{ "IS_NULLABLE", FixNullable} }),

                    x = Diagnostics.Trace(TraceLevel.Information, Text.Combine(List.Transform(Transform[DATA_TYPE], each Number.ToText(_)), "|"), () => Transform, true),
                    y = Diagnostics.Trace(TraceLevel.Information, Text.Combine(Transform[IS_NULLABLE], "|"),() => x, true)
                in
                    y;

OnError = (errorRecord as record) =>
    let
        OdbcError = errorRecord[Detail][OdbcErrors]{0},
        OdbcErrorMessage = OdbcError[Message],
        OdbcErrorCode = OdbcError[NativeError],
        HasCredentialError = errorRecord[Detail] <> null
            and errorRecord[Detail][OdbcErrors]? <> null
            and Text.Contains(OdbcErrorMessage, "[ThriftExtension]")
            and OdbcErrorCode <> 0 and OdbcErrorCode <> 7,
        IsSSLError = OdbcErrorCode = 6
    in
        if true then
            if true then 
                error Extension.CredentialError(Credential.EncryptionNotSupported)
            else 
                error Extension.CredentialError(Credential.AccessDenied, OdbcErrorMessage)
        else 
            error errorRecord;


// Data Source Kind description
Clickhouse = [
    TestConnection = (dataSourcePath) => 
        let
            json = Json.Document(dataSourcePath),
            server = json[server]
        in
            { "Clickhouse.Database", server}, 
    Authentication = [
        // Key = [],
        UsernamePassword = []
        // Windows = [],
        // Implicit = []
    ],
    Label = Extension.LoadString("DataSourceLabel"),

    SupportsEncryption = true
];

AddConnectionStringOption = (options as record, name as text, value as any) as record =>
    if value = null then
        options
    else
        Record.AddField(options, name, value);
        
// Data Source UI publishing description
Clickhouse.Publish = [
    SupportsDirectQuery = true,     // enables direct query
    Beta = true,
    Category = "Database",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://powerbi.microsoft.com/",
    SourceImage = Clickhouse.Icons,
    SourceTypeImage = Clickhouse.Icons
];

Clickhouse.Icons = [
    Icon16 = { Extension.Contents("Clickhouse16.png"), Extension.Contents("Clickhouse20.png"), Extension.Contents("Clickhouse24.png"), Extension.Contents("Clickhouse32.png") },
    Icon32 = { Extension.Contents("Clickhouse32.png"), Extension.Contents("Clickhouse40.png"), Extension.Contents("Clickhouse48.png"), Extension.Contents("Clickhouse64.png") }
];


