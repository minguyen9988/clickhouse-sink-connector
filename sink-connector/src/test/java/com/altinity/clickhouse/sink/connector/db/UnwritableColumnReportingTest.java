package com.altinity.clickhouse.sink.connector.db;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfigVariables;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
     * Config with the connection pool disabled.
     *
     * <p>Only used by the rejected-ALTER test. {@code executeSystemQuery}
     * retries a failing statement with a growing sleep and, with pooling on,
     * also tries to obtain a fresh connection on each attempt -- which takes
     * ~45s of wall clock for an outcome that cannot change. Disabling the
     * pool keeps the same code path and the same verdict without the test
     * paying for the backoff.</p>
     */
    private static ClickHouseSinkConnectorConfig noPoolConfig() {
        HashMap<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnectorConfigVariables
                .CONNECTION_POOL_DISABLE.toString(), "true");
        return new ClickHouseSinkConnectorConfig(props);
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

    /**
     * getColumnType must restate the declared type verbatim -- MODIFY COLUMN
     * has to repeat it, and inventing it would silently change the column.
     */
    @Test
    public void testColumnTypeIsReadVerbatim() {
        String type = new DBMetadata(config()).getColumnType(
                "orders", "sales", "total_with_tax",
                connectionReturning("Nullable(Decimal(30, 10))"));
        Assert.assertEquals("Nullable(Decimal(30, 10))", type);
    }

    /**
     * Enforcement: the connector rewrites the column definition so the source
     * value can be stored. This is the whole point -- the replica is made to
     * conform, rather than the divergence being reported and left in place.
     */
    @Test
    public void testMakeColumnWritableIssuesModifyColumn() {
        List<String> issued = new ArrayList<>();
        boolean ok = new DBMetadata(config()).makeColumnWritable(
                "orders", "sales", "total_with_tax", "Nullable(UInt8)",
                recordingConnection(issued, false));

        Assert.assertTrue("enforcement should report success", ok);
        Assert.assertEquals(1, issued.size());
        Assert.assertEquals(
                "ALTER TABLE `sales`.`orders` MODIFY COLUMN `total_with_tax` "
                        + "Nullable(UInt8)",
                issued.get(0));
    }

    /**
     * Identifiers are backticked: replicated table and column names routinely
     * contain characters ClickHouse would otherwise parse.
     */
    @Test
    public void testEnforcementQuotesIdentifiers() {
        List<String> issued = new ArrayList<>();
        new DBMetadata(config()).makeColumnWritable(
                "order-items", "sales db", "total.with.tax", "String",
                recordingConnection(issued, false));

        Assert.assertEquals(1, issued.size());
        Assert.assertTrue(issued.get(0),
                issued.get(0).contains("`sales db`.`order-items`"));
        Assert.assertTrue(issued.get(0),
                issued.get(0).contains("`total.with.tax`"));
    }

    /**
     * A rejected DDL (no privilege, unsupported change) must report failure
     * rather than throwing, so the caller falls back to warning instead of
     * failing a batch that would otherwise have succeeded.
     */
    @Test
    public void testEnforcementFailureIsReportedNotThrown() {
        List<String> issued = new ArrayList<>();
        boolean ok = new DBMetadata(noPoolConfig()).makeColumnWritable(
                "orders", "sales", "total_with_tax", "Nullable(UInt8)",
                recordingConnection(issued, true));

        Assert.assertFalse("a rejected ALTER must not report success", ok);
    }

    /**
     * The dangerous case: the ALTER is submitted and no exception surfaces,
     * but the column is STILL MATERIALIZED afterwards.
     *
     * <p>{@code executeSystemQuery} retries a failing statement and then
     * returns normally once the retry budget is spent, so "no exception" does
     * not mean "it worked". Reporting success here would be the worst
     * outcome available: the caller would believe the replica had been
     * corrected and stop warning, while the source value went on being
     * silently discarded. Enforcement must therefore be verified, not
     * assumed.</p>
     */
    @Test
    public void testSilentlyIneffectiveAlterIsNotReportedAsSuccess() {
        List<String> issued = new ArrayList<>();
        boolean ok = new DBMetadata(config()).makeColumnWritable(
                "orders", "sales", "total_with_tax", "Nullable(UInt8)",
                ineffectiveAlterConnection(issued));

        Assert.assertEquals("the ALTER was submitted", 1, issued.size());
        Assert.assertFalse(
                "an ALTER that did not change default_kind is not success", ok);
    }

    /**
     * A connection that accepts the ALTER without error but reports the
     * column as still MATERIALIZED, i.e. the statement had no effect.
     */
    private static Connection ineffectiveAlterConnection(
            final List<String> issued) {
        InvocationHandler connection = (proxy, method, args) -> {
            String name = method.getName();
            if ("prepareStatement".equals(name) && args != null && args.length > 0) {
                issued.add(String.valueOf(args[0]));
                InvocationHandler prepared = (p2, m2, a2) ->
                        "execute".equals(m2.getName())
                                ? Boolean.FALSE : defaultFor(m2.getReturnType());
                return Proxy.newProxyInstance(
                        UnwritableColumnReportingTest.class.getClassLoader(),
                        new Class<?>[]{PreparedStatement.class}, prepared);
            }
            if ("createStatement".equals(name)) {
                final boolean[] consumed = {false};
                InvocationHandler resultSet = (p3, m3, a3) -> {
                    switch (m3.getName()) {
                        case "next":
                            if (consumed[0]) {
                                return false;
                            }
                            consumed[0] = true;
                            return true;
                        case "getString":
                            return "MATERIALIZED";
                        case "close":
                            return null;
                        default:
                            return defaultFor(m3.getReturnType());
                    }
                };
                final ResultSet rs = (ResultSet) Proxy.newProxyInstance(
                        UnwritableColumnReportingTest.class.getClassLoader(),
                        new Class<?>[]{ResultSet.class}, resultSet);
                InvocationHandler stmt = (p4, m4, a4) ->
                        "executeQuery".equals(m4.getName())
                                ? rs : defaultFor(m4.getReturnType());
                return Proxy.newProxyInstance(
                        UnwritableColumnReportingTest.class.getClassLoader(),
                        new Class<?>[]{Statement.class}, stmt);
            }
            return defaultFor(method.getReturnType());
        };
        return (Connection) Proxy.newProxyInstance(
                UnwritableColumnReportingTest.class.getClassLoader(),
                new Class<?>[]{Connection.class}, connection);
    }

    /** Enforcement never runs on incomplete inputs. */
    @Test
    public void testEnforcementIsInertOnMissingInputs() {
        List<String> issued = new ArrayList<>();
        DBMetadata m = new DBMetadata(config());

        Assert.assertFalse(m.makeColumnWritable(null, "sales", "c", "UInt8",
                recordingConnection(issued, false)));
        Assert.assertFalse(m.makeColumnWritable("orders", null, "c", "UInt8",
                recordingConnection(issued, false)));
        Assert.assertFalse(m.makeColumnWritable("orders", "sales", null, "UInt8",
                recordingConnection(issued, false)));
        Assert.assertFalse(m.makeColumnWritable("orders", "sales", "c", null,
                recordingConnection(issued, false)));
        Assert.assertFalse(m.makeColumnWritable("orders", "sales", "c", "",
                recordingConnection(issued, false)));
        Assert.assertFalse(m.makeColumnWritable("orders", "sales", "c", "UInt8",
                null));

        Assert.assertTrue("no DDL may be issued for incomplete inputs",
                issued.isEmpty());
    }

    /**
     * A connection that records every statement executed, optionally
     * rejecting them to simulate a refused ALTER.
     *
     * <p>Records the SQL at {@code prepareStatement}, because that is how
     * {@code DBMetadata#executeSystemQuery} submits DDL: it prepares the
     * statement and calls {@code execute()} (not {@code executeQuery()}),
     * since clickhouse-jdbc >= 0.9.x rejects {@code executeQuery} for
     * statements that return no ResultSet. A stub that only implements
     * {@code createStatement} never observes the call at all.</p>
     */
    private static Connection recordingConnection(final List<String> issued,
                                                  final boolean reject) {
        // What the post-ALTER verification read sees. When the ALTER is
        // rejected the column is still MATERIALIZED; when it succeeds the
        // default_kind is empty, i.e. an ordinary writable column.
        final String kindAfter = reject ? "MATERIALIZED" : "";

        InvocationHandler connection = (proxy, method, args) -> {
            String name = method.getName();

            // DDL submission path: executeSystemQuery prepares and execute()s.
            if ("prepareStatement".equals(name) && args != null && args.length > 0) {
                if (reject) {
                    // The real shape of a denied ALTER. Code 497 is
                    // non-retryable, so executeSystemQuery surfaces it at once
                    // instead of sleeping out the retry budget on the CDC
                    // thread -- which is the behaviour this asserts.
                    throw new SQLException("Code: 497. DB::Exception: "
                            + "sink_connector: Not enough privileges. To execute "
                            + "this query, it's necessary to have the grant "
                            + "ALTER MODIFY COLUMN ON sales.orders. "
                            + "(ACCESS_DENIED)");
                }
                issued.add(String.valueOf(args[0]));
                InvocationHandler prepared = (p2, m2, a2) ->
                        "execute".equals(m2.getName())
                                ? Boolean.FALSE : defaultFor(m2.getReturnType());
                return Proxy.newProxyInstance(
                        UnwritableColumnReportingTest.class.getClassLoader(),
                        new Class<?>[]{PreparedStatement.class}, prepared);
            }

            // Verification path: getColumnDefaultKind re-reads system.columns
            // through createStatement().executeQuery(...).
            if ("createStatement".equals(name)) {
                final boolean[] consumed = {false};
                InvocationHandler resultSet = (p3, m3, a3) -> {
                    switch (m3.getName()) {
                        case "next":
                            if (consumed[0]) {
                                return false;
                            }
                            consumed[0] = true;
                            return true;
                        case "getString":
                            return kindAfter;
                        case "close":
                            return null;
                        default:
                            return defaultFor(m3.getReturnType());
                    }
                };
                final ResultSet rs = (ResultSet) Proxy.newProxyInstance(
                        UnwritableColumnReportingTest.class.getClassLoader(),
                        new Class<?>[]{ResultSet.class}, resultSet);
                InvocationHandler stmt = (p4, m4, a4) ->
                        "executeQuery".equals(m4.getName())
                                ? rs : defaultFor(m4.getReturnType());
                return Proxy.newProxyInstance(
                        UnwritableColumnReportingTest.class.getClassLoader(),
                        new Class<?>[]{Statement.class}, stmt);
            }

            return defaultFor(method.getReturnType());
        };
        return (Connection) Proxy.newProxyInstance(
                UnwritableColumnReportingTest.class.getClassLoader(),
                new Class<?>[]{Connection.class}, connection);
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
