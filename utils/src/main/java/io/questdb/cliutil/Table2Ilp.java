/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cliutil;

import io.questdb.cairo.TableUtils;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;

public class Table2Ilp {

    public static void main(String[] args) {
        LogFactory.enableGuaranteedLogging();

        try {
            Table2IlpParams params = Table2IlpParams.parse(args);
            if (!params.isValid()) {
                printUsage();
                return;
            }

            new Table2IlpCopier().copyTable(params);
        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
            printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("usage: " + Table2Ilp.class.getName() + " -d <destination_table_name> -dc <destination_ilp_host_port> -s <source_select_query> -sc <source_pg_connection_string> \\ " +
                "\n [-sts <timestamp_column>] [-sym <symbol_columns>] [-dauth <ilp_auth_key:ilp_auth_token>] [-dtls]");
    }

    static class Table2IlpParams {
        private String destinationAuthKey;
        private String destinationAuthToken;
        private boolean destinationEnableTls;
        private String destinationIlpHost;
        private int destinationIlpPort;
        private String destinationTableName;
        private String sourcePgConnectionString;
        private String sourceSelectQuery;
        private String sourceTimestampColumnName = "timestamp";
        private String[] symbols;
        private boolean valid = false;

        public static Table2IlpParams parse(String[] args) throws IllegalArgumentException {
            Table2IlpParams params = new Table2IlpParams();
            if (args.length < 8 || args.length > 15) {
                throw new IllegalArgumentException("Invalid number of arguments");
            }

            String symbolColumns = null, destinationIlpHostPort = null, destinationAuth = null;

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "-d":
                        params.destinationTableName = args[++i].trim();
                        break;
                    case "-dc":
                        destinationIlpHostPort = args[++i].trim();
                        break;
                    case "-s":
                        params.sourceSelectQuery = args[++i].trim();
                        break;
                    case "-sc":
                        params.sourcePgConnectionString = args[++i].trim();
                        break;
                    case "-sym":
                        symbolColumns = args[++i].trim();
                        break;
                    case "-dauth":
                        destinationAuth = args[++i].trim();
                        break;
                    case "-dtls":
                        params.destinationEnableTls = true;
                        break;
                    case "-sts":
                        params.sourceTimestampColumnName = args[++i].trim();
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid token: " + arg);
                }
            }

            if (!TableUtils.isValidColumnName(params.sourceTimestampColumnName, 255)) {
                throw new IllegalArgumentException("Invalid timestamp column name: " + params.sourceTimestampColumnName);
            }

            if (!TableUtils.isValidTableName(params.destinationTableName, 255)) {
                throw new IllegalArgumentException("Invalid destination table name: " + params.destinationTableName);
            }

            if (destinationIlpHostPort == null) {
                throw new IllegalArgumentException("Destination ILP host:port not specified");
            }

            String[] parts = destinationIlpHostPort.split("\\s*:\\s*");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid destination ILP host:port '" + destinationIlpHostPort + "'");
            }
            params.destinationIlpHost = parts[0];
            try {
                params.destinationIlpPort = Numbers.parseInt(parts[1]);
            } catch (NumericException e) {
                throw new IllegalArgumentException("Invalid destination ILP port: " + destinationIlpHostPort);
            }

            if (params.sourcePgConnectionString == null) {
                throw new IllegalArgumentException("Empty source Postgres connection string");
            }

            if (symbolColumns != null) {
                String[] symbolColumnNames = symbolColumns.split("\\s*,\\s*");
                for (String columnName : symbolColumnNames) {
                    if (!TableUtils.isValidColumnName(columnName, 255)) {
                        throw new IllegalArgumentException("Invalid symbol column name: " + columnName);
                    }
                }
                params.symbols = symbolColumnNames;
            } else {
                params.symbols = new String[0];
            }

            if (destinationAuth != null) {
                String[] authParts = destinationAuth.split("\\s*:\\s*");
                if (authParts.length != 2) {
                    throw new IllegalArgumentException("Invalid destination ILP auth key:token '" + destinationAuth + "'");
                }
                params.destinationAuthKey = authParts[0];
                params.destinationAuthToken = authParts[1];
            }

            params.valid = true;
            return params;
        }

        public boolean enableDestinationTls() {
            return destinationEnableTls;
        }

        public String getDestinationAuthKey() {
            return destinationAuthKey;
        }

        public String getDestinationAuthToken() {
            return destinationAuthToken;
        }

        public String getDestinationIlpHost() {
            return destinationIlpHost;
        }

        public int getDestinationIlpPort() {
            return destinationIlpPort;
        }

        public String getDestinationTableName() {
            return destinationTableName;
        }

        public String getSourcePgConnectionString() {
            return sourcePgConnectionString;
        }

        public String getSourceSelectQuery() {
            return sourceSelectQuery;
        }

        public String getSourceTimestampColumnName() {
            return sourceTimestampColumnName;
        }

        public String[] getSymbols() {
            return symbols;
        }

        public boolean isValid() {
            return valid;
        }
    }
}

