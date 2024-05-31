import io.questdb.client.Sender;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

import java.sql.*;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class Table2IlpCopier {

    public long copyTable(Table2Ilp.Table2IlpParams params) {
        LowerCaseCharSequenceHashSet symbols = createSymbolsSet(params);
        MicrosecondClock clock = new MicrosecondClockImpl();
        long totalLines = 0;
        try (Connection conn = getConnection(params.getSourcePgConnectionString())) {
            conn.setAutoCommit(false);
            try (PreparedStatement stmt = conn.prepareStatement(params.getSourceSelectQuery())) {
                stmt.setFetchSize(8 * 1024);
                long start = clock.getTicks();
                try (ResultSet rs = stmt.executeQuery(); Sender sender = buildSender(params)) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();
                    String[] columnNames = new String[columnCount];
                    int[] columnTypes = new int[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        columnNames[i] = meta.getColumnName(i + 1);
                        columnTypes[i] = meta.getColumnType(i + 1);
                    }
                    int tsIndex = getTimestampIndex(columnNames, columnTypes, params.getSourceTimestampColumnName());
                    while (rs.next()) {
                        sendLine(symbols, rs, columnCount, columnNames, columnTypes, tsIndex, sender, params.getDestinationTableName());
                        totalLines++;
                        if (totalLines % 10000 == 0) {
                            long end = clock.getTicks();
                            long linesPerSec = 10000 * 1_000_000 / (end - start);
                            System.out.println(totalLines + " lines, " + linesPerSec + " lines/sec");
                            start = clock.getTicks();
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("Failed to connect to the source");
        } finally {
            System.out.println("Total lines sent: " + totalLines);
        }
        return totalLines;
    }

    private static Sender buildSender(Table2Ilp.Table2IlpParams params) {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.TCP);
        builder.address(params.getDestinationIlpHost() + ":" + params.getDestinationIlpPort());
        if (params.enableDestinationTls()) {
            builder.enableTls();
        }
        if (params.getDestinationAuthKey() != null) {
            builder.enableAuth(params.getDestinationAuthKey())
                   .authToken(params.getDestinationAuthToken());
        }
        return builder.build();
    }

    private static LowerCaseCharSequenceHashSet createSymbolsSet(Table2Ilp.Table2IlpParams params) {
        LowerCaseCharSequenceHashSet set = new LowerCaseCharSequenceHashSet();
        for (String symbol : params.getSymbols()) {
            set.add(symbol);
        }
        return set;
    }

    private static Connection getConnection(String connectionString) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "admin");
        props.setProperty("password", "quest");
        props.setProperty("sslmode", "disable");
        props.setProperty("binaryTransfer", "true");
        props.setProperty("preferQueryMode", "extendedForPrepared");
        return DriverManager.getConnection(connectionString, props);
    }

    private static long getMicroEpoch(ResultSet rs, int tsIndex) throws SQLException {
        String ts = rs.getString(tsIndex);
        return (ts != null) ? IntervalUtils.parseFloorPartialTimestamp(ts) : Long.MIN_VALUE;
    }

    private static int getTimestampIndex(String[] colNames, int[] colTypes, String tsColName) {
        String lcTsColName = tsColName.toLowerCase();
        for (int i = 0; i < colNames.length; i++) {
            if (colNames[i].toLowerCase().equals(lcTsColName) && 
                (colTypes[i] == Types.TIMESTAMP || colTypes[i] == Types.DATE)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Source timestamp column '" + tsColName + "' not found");
    }

    private static void sendLine(LowerCaseCharSequenceHashSet symbols, ResultSet rs, int colCount,
                                 String[] colNames, int[] colTypes, int tsIndex, Sender sender, String tableName) throws SQLException {
        sender.table(tableName);
        for (int i = 0; i < colCount; i++) {
            String colName = colNames[i];
            if (symbols.contains(colName)) {
                String value = rs.getString(i + 1);
                if (value != null) {
                    sender.symbol(colName, value);
                }
            }
        }
        for (int i = 0; i < colCount; i++) {
            String colName = colNames[i];
            if (!symbols.contains(colName)) {
                switch (colTypes[i]) {
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                        long valLong = rs.getLong(i + 1);
                        if (!rs.wasNull()) {
                            sender.longColumn(colName, valLong);
                        }
                        break;
                    case Types.DOUBLE:
                    case Types.FLOAT:
                    case Types.REAL:
                        double valDouble = rs.getDouble(i + 1);
                        if (!rs.wasNull() && !Double.isNaN(valDouble)) {
                            sender.doubleColumn(colName, valDouble);
                        }
                        break;
                    case Types.BOOLEAN:
                    case Types.BIT:
                        boolean valBoolean = rs.getBoolean(i + 1);
                        if (!rs.wasNull()) {
                            sender.boolColumn(colName, valBoolean);
                        }
                        break;
                    case Types.DATE:
                    case Types.TIMESTAMP:
                        if (i != tsIndex) {
                            long tsMicroEpoch = getMicroEpoch(rs, i + 1);
                            if (tsMicroEpoch != Long.MIN_VALUE && !rs.wasNull()) {
                                sender.timestampColumn(colName, tsMicroEpoch, ChronoUnit.MICROS);
                            }
                        }
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                        String valString = rs.getString(i + 1);
                        if (valString != null && !rs.wasNull()) {
                            sender.stringColumn(colName, valString);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported column type: " + colName);
                }
            }
        }
        long tsMicroEpoch = getMicroEpoch(rs, tsIndex + 1);
        sender.at(tsMicroEpoch, ChronoUnit.MICROS);
    }
}
