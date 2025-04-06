package liquibase.database.core;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.changelog.column.LiquibaseColumn;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.logging.Logger;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawCallStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Table;
import liquibase.util.JdbcUtil;
import liquibase.util.StringUtil;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * 该类需要为 Liquibase 注册你的自定义数据库类，Liquibase 在启动时加载并注册 KingbaseDatabase 类
 * META-INF/services/liquibase.database.Database
 *
 * @author huangxz created on 2024/09/10.
 */
public class KingbaseDatabase extends AbstractJdbcDatabase {

    public static final String PRODUCT_NAME = "KingbaseES";
    private static final int KINGBASE_DEFAULT_TCP_PORT_NUMBER = 54321;
    private static final Logger LOG = Scope.getCurrentScope().getLog(KingbaseDatabase.class);

    private Set<String> systemTablesAndViews = new HashSet<>();

    private Set<String> reservedWords = new HashSet<>();
    private Boolean supportBytea;

    public KingbaseDatabase(){

        setCurrentDateTimeFunction("NOW()");
        reservedWords.addAll(Arrays.asList("ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC",
                "ASYMMETRIC", "AUTHORIZATION", "BINARY", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLLATION",
                "COLUMN", "CONCURRENTLY", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_CATALOG", "CURRENT_DATE",
                "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT",
                "DEFERRABLE", "DESC", "DISTINCT", "DO", "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN",
                "FREEZE", "FROM", "FULL", "GRANT", "GROUP", "HAVING", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT",
                "INTO", "IS", "ISNULL", "JOIN", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LOCALTIME",
                "LOCALTIMESTAMP", "NATURAL", "NOT", "NOTNULL", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER",
                "OVERLAPS", "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "RIGHT", "SELECT", "SESSION_USER",
                "SIMILAR", "SOME", "SYMMETRIC", "TABLE", "TABLESAMPLE", "THEN", "TO", "TRAILING", "TRUE", "UNION",
                "UNIQUE", "USER", "USING", "VARIADIC", "VERBOSE", "WHEN", "WHERE", "WINDOW", "WITH"));

        dateFunctions.add(new DatabaseFunction("SYSDATE"));
        dateFunctions.add(new DatabaseFunction("SYSTIMESTAMP"));
        dateFunctions.add(new DatabaseFunction("CURRENT_TIMESTAMP"));

        sequenceNextValueFunction = "nextval('%s')";
        sequenceCurrentValueFunction = "currval('%s')";

        unmodifiableDataTypes.addAll(Arrays.asList("bool", "int4", "int8", "float4", "float8", "bigserial", "serial", "oid", "bytea", "date", "timestamptz", "text","varchar"));

        unquotedObjectsAreUppercased=false;

    }




    @Override
    public boolean equals(Object o) {
        // Actually, we don't need and more specific checks than the base method. This exists just to make SONAR happy.
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        // Actually, we don't need and more specific hashing than the base method. This exists just to make SONAR happy.
        return super.hashCode();
    }

    @Override
    public Set<String> getSystemViews() {
        return systemTablesAndViews;
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }


    /**
     * 该方法判断是否为liquibase实现的database
     * @param conn
     * @return
     * @throws DatabaseException
     */
    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:kingbase8:")) {
            return "com.kingbase8.Driver";
        }
        return null;
    }

    /**
     * 返回数据库的短名称，在liquibase.properties中指定数据库类型
     * @return
     */
    @Override
    public String getShortName() {
        return "kingbase";
    }

    @Override
    public Integer getDefaultPort() {
        return KINGBASE_DEFAULT_TCP_PORT_NUMBER;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    /**
     * 设置高优先级，以便 Liquibase 选择这个类
     * @return
     */
    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT + 5;
    }

    @Override
    public boolean supportsCatalogInObjectName(Class<? extends DatabaseObject> type) {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return true;
    }

    @Override
    public String getDatabaseChangeLogTableName() {
        return super.getDatabaseChangeLogTableName().toLowerCase(Locale.US);
    }

    @Override
    public String getDatabaseChangeLogLockTableName() {
        return super.getDatabaseChangeLogLockTableName().toLowerCase(Locale.US);
    }

    @Override
    public void setConnection(DatabaseConnection conn) {
        super.setConnection(conn);


        if (conn instanceof JdbcConnection) {
            Statement statement = null;
            ResultSet resultSet = null;
            try {
                statement = ((JdbcConnection) conn).createStatement();
                resultSet = statement.executeQuery("select setting from sys_settings where name = 'date'");
                if (resultSet.next()) {
                    String setting = resultSet.getString(1);
                    if ("on".equals(setting)) {
                        LOG.warning("KingbaseES " + conn.getURL() + " does not store DATE columns. Instead, it auto-converts " +
                                "them " +
                                "to TIMESTAMPs. (date=true)");
                    }
                }
            } catch (SQLException | DatabaseException e) {
                LOG.info("Cannot check sys_settings", e);
            } finally {
                JdbcUtil.close(resultSet, statement);
            }
        }

    }

    @Override
    public boolean isSystemObject(DatabaseObject example) {
        // All tables in the schemas pg_catalog and pg_toast are definitely system tables.
        if(
                (example instanceof Table) && (example.getSchema() != null)   && (
                        ("SYS_CATALOG".equals(example.getSchema().getName())) || ("SYS_TOAST".equals(example.getSchema().getName()))
                )
        ) {
            return true;
        }

        return super.isSystemObject(example);
    }

    @Override
    public String getAutoIncrementClause() {
        if (useSerialDatatypes()) {
            return "";
        }

        return super.getAutoIncrementClause();
    }

    @Override
    protected String getAutoIncrementClause(final String generationType, final Boolean defaultOnNull) {
        if (useSerialDatatypes()) {
            return "";
        }

        if (StringUtil.isEmpty(generationType)) {
            return super.getAutoIncrementClause();
        }

        String autoIncrementClause = "GENERATED %s AS IDENTITY"; // %s -- [ ALWAYS | BY DEFAULT ]
        return String.format(autoIncrementClause, generationType);
    }

    @Override
    public boolean generateAutoIncrementStartWith(BigInteger startWith) {
        if (useSerialDatatypes()) {
            return false;
        }
        return super.generateAutoIncrementStartWith(startWith);
    }

    @Override
    public boolean generateAutoIncrementBy(BigInteger incrementBy) {
        if (useSerialDatatypes()) {
            return false;
        }

        return super.generateAutoIncrementBy(incrementBy);
    }

    /**
     * Should the database use "serial" datatypes vs. "generated by default as identity"
     */
    public boolean useSerialDatatypes() {
        try {
            return getDatabaseMajorVersion() < 10;
        } catch (DatabaseException e) {
            return true;
        }
    }

    @Override
    public String escapeObjectName(String objectName, Class<? extends DatabaseObject> objectType) {
        if ((quotingStrategy == ObjectQuotingStrategy.LEGACY) && hasMixedCase(objectName)) {
            return "\"" + objectName + "\"";
        } else if (objectType != null && LiquibaseColumn.class.isAssignableFrom(objectType)) {
            return (objectName != null && !objectName.isEmpty()) ? objectName.trim() : objectName;
        }

        return super.escapeObjectName(objectName, objectType);
    }

    @Override
    public String correctObjectName(String objectName, Class<? extends DatabaseObject> objectType) {
        if ((objectName == null) || (quotingStrategy != ObjectQuotingStrategy.LEGACY)) {
            return super.correctObjectName(objectName, objectType);
        }
        if (objectName.contains("-")
                || hasMixedCase(objectName)
                || startsWithNumeric(objectName)
                || isReservedWord(objectName)) {
            return objectName;
        } else {
            return objectName.toLowerCase(Locale.US);
        }
    }

    protected boolean hasMixedCase(String tableName) {
        if (tableName == null) {
            return false;
        }
        return StringUtil.hasUpperCase(tableName) && StringUtil.hasLowerCase(tableName);
    }

    @Override
    public boolean isReservedWord(String tableName) {
        return reservedWords.contains(tableName.toUpperCase());
    }

    @Override
    protected SqlStatement getConnectionSchemaNameCallStatement() {
        return new RawCallStatement("select current_schema()");
    }

    @Override
    public String generatePrimaryKeyName(final String tableName) {
        return tableName.toUpperCase(Locale.US) + "_PKEY";
    }

    @Override
    public CatalogAndSchema.CatalogAndSchemaCase getSchemaAndCatalogCase() {
        return CatalogAndSchema.CatalogAndSchemaCase.LOWER_CASE;
    }

    @Override
    public void rollback() throws DatabaseException {
        super.rollback();

        // Rollback in postgresql resets the search path. Need to put it back to the defaults
        // Prevent resetting the search path if we are running in a mode that does not update the database to avoid spurious
        // SET SEARCH_PATH SQL statements
        final Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", this);
        if(executor.updatesDatabase()) {
            DatabaseUtils.initializeDatabase(getDefaultCatalogName(), getDefaultSchemaName(), this);
        }
    }

    @Override
    public void setDefaultCatalogName(String defaultCatalogName) {
        if (StringUtil.isNotEmpty(defaultCatalogName)) {
            Scope.getCurrentScope().getUI().sendMessage("WARNING: Kingbase does not support catalogs, so the values set in 'defaultCatalogName' and 'referenceDefaultCatalogName' will be ignored.");
        }
        super.setDefaultCatalogName(defaultCatalogName);
    }

    @Override
    public boolean supportsCreateIfNotExists(Class<? extends DatabaseObject> type) {
        return type.isAssignableFrom(Table.class);
    }

    @Override
    public boolean supportsDatabaseChangeLogHistory() {
        return true;
    }


}
