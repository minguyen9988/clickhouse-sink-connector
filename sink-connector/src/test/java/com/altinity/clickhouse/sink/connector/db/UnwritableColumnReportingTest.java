package com.altinity.clickhouse.sink.connector.db;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

/**
 * Pins {@link DBMetadata#getColumnDefaultKind}, which is what lets the
 * connector tell an ALIAS column apart from a MATERIALIZED one.
 *
 * <p>The distinction matters because this connector replicates a source
 * database into ClickHouse, making the source the authority on what the data
 * is. {@code getColumnsDataTypesForTable} excludes both kinds from the
 * writable column map -- binding either makes ClickHouse reject the INSERT --
 * but only one of them causes a divergence:</p>
 *
 * <ul>
 *   <li>ALIAS stores nothing, so there is no stored value that can disagree
 *       with the source.</li>
 *   <li>MATERIALIZED stores a value ClickHouse computed. When the source also
 *       supplies that column, the replica silently keeps a different value --
 *       no error, no failed batch, identical row counts, detectable only by a
 *       value-level checksum.</li>
 * </ul>
 *
 * <p>Without a kind-aware lookup both cases collapse into "not writable, must
 * be fine", which is how a real divergence gets logged as correct behaviour.</p>
 *
 * <p>The JDBC objects are dynamic proxies rather than a mocking framework:
 * this module has no Mockito dependency, and only three methods are needed.</p>
 */
public class UnwritableColumnReportingTest {

    private static ClickHouseSinkConnectorConfig config() {
        return new ClickHouseSinkConnectorConfig(new HashMap<>());
    }

    /**
     * Builds a connection whose statement returns one default_kind row, or no
     * rows when {@code kind} is null (the column does not exist).
     */
    private static Connection connectionReturning(final String kind) {
        final boolean[] consumed = {false};

        InvocationHandler resultSet = (proxy, method, args) -> {
            switch (method.getName()) {
                case "next":
                    if (kind == null || consumed[0]) {
                        return false;
                    }
                    consumed[0] = true;
                    return true;
                case "getString":
                    return kind;
                case "close":
                    return null;
                default:
                    return defaultFor(method.getReturnType());
            }
        };
        final ResultSet rs = (ResultSet) Proxy.newProxyInstance(
                UnwritableColumnReportingTest.class.getClassLoader(),
                new Class<?>[]{ResultSet.class}, resultSet);

        InvocationHandler statement = (proxy, method, args) ->
                "executeQuery".equals(method.getName())
                        ? rs : defaultFor(method.getReturnType());
        final Statement stmt = (Statement) Proxy.newProxyInstance(
                UnwritableColumnReportingTest.class.getClassLoader(),
                new Class<?>[]{Statement.class}, statement);

        InvocationHandler connection = (proxy, method, args) ->
                "createStatement".equals(method.getName())
                        ? stmt : defaultFor(method.getReturnType());
        return (Connection) Proxy.newProxyInstance(
                UnwritableColumnReportingTest.class.getClassLoader(),
                new Class<?>[]{Connection.class}, connection);
    }

    /** A connection whose createStatement() always fails. */
    private static Connection failingConnection() {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("createStatement".equals(method.getName())) {
                throw new SQLException("metadata unavailable");
            }
            return defaultFor(method.getReturnType());
        };
        return (Connection) Proxy.newProxyInstance(
                UnwritableColumnReportingTest.class.getClassLoader(),
                new Class<?>[]{Connection.class}, handler);
    }

    /** Zero value for a primitive return type, null otherwise. */
    private static Object defaultFor(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == void.class) {
            return null;
        }
        return 0;
    }

    /** A MATERIALIZED column must be identified as such -- it is the divergent case. */
    @Test
    public void testMaterializedKindIsReported() {
        String kind = new DBMetadata(config()).getColumnDefaultKind(
                "orders", "sales", "total_with_tax", connectionReturning("MATERIALIZED"));
        Assert.assertEquals("MATERIALIZED", kind);
    }

    /** An ALIAS column must be identified as such -- it stores nothing. */
    @Test
    public void testAliasKindIsReported() {
        String kind = new DBMetadata(config()).getColumnDefaultKind(
                "orders", "sales", "display_name", connectionReturning("ALIAS"));
        Assert.assertEquals("ALIAS", kind);
    }

    /**
     * An ordinary column reports an empty default_kind, which must come back
     * as "" rather than null -- null is reserved for "could not determine".
     */
    @Test
    public void testOrdinaryColumnReportsEmptyKind() {
        String kind = new DBMetadata(config()).getColumnDefaultKind(
                "orders", "sales", "order_id", connectionReturning(""));
        Assert.assertEquals("", kind);
    }

    /** A column ClickHouse does not have at all yields null, not "". */
    @Test
    public void testMissingColumnYieldsNull() {
        String kind = new DBMetadata(config()).getColumnDefaultKind(
                "orders", "sales", "no_such_column", connectionReturning(null));
        Assert.assertNull(kind);
    }

    /**
     * A metadata failure must degrade to null rather than propagating: this
     * runs on the write path, and a diagnostic lookup must never be able to
     * fail a batch that would otherwise have succeeded.
     */
    @Test
    public void testMetadataFailureYieldsNullRatherThanThrowing() {
        String kind = new DBMetadata(config()).getColumnDefaultKind(
                "orders", "sales", "total_with_tax", failingConnection());
        Assert.assertNull(kind);
    }

    /** Null arguments are inert -- the lookup sits on the hot path. */
    @Test
    public void testNullArgumentsAreInert() {
        DBMetadata metadata = new DBMetadata(config());
        Connection conn = connectionReturning("MATERIALIZED");

        Assert.assertNull(metadata.getColumnDefaultKind(null, "sales", "c", conn));
        Assert.assertNull(metadata.getColumnDefaultKind("orders", null, "c", conn));
        Assert.assertNull(metadata.getColumnDefaultKind("orders", "sales", null, conn));
        Assert.assertNull(metadata.getColumnDefaultKind("orders", "sales", "c", null));
    }
}
