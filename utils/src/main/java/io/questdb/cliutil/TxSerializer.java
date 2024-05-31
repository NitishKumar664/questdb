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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import static io.questdb.cairo.TableUtils.*;

public class TxSerializer {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final FilesFacade ff = new FilesFacadeImpl();

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            printUsage();
            return;
        }

        TxSerializer serializer = new TxSerializer();
        if ("-s".equals(args[0])) {
            if (args.length != 3) {
                printUsage();
                return;
            }
            serializer.serializeFile(args[1], args[2]);
        } else if ("-d".equals(args[0])) {
            String json = serializer.toJson(args[1]);
            if (json != null) {
                System.out.println(json);
            }
        } else {
            printUsage();
        }
    }

    public void serializeFile(String jsonFile, String target) throws IOException {
        String json = new String(Files.readAllBytes(Paths.get(jsonFile)), StandardCharsets.UTF_8);
        serializeJson(json, target);
    }

    public String toJson(String srcTxFilePath) {
        TxFileStruct tx = new TxFileStruct();

        try (MemoryMR roTxMem = Vm.getMRInstance(ff, srcTxFilePath, ff.length(srcTxFilePath), MemoryTag.MMAP_DEFAULT)) {
            roTxMem.growToFileSize();
            // Parse transaction data
            parseTransactionData(roTxMem, tx);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return GSON.toJson(tx);
    }

    private void parseTransactionData(MemoryMR roTxMem, TxFileStruct tx) {
        // Parse transaction data here
    }

    private static void printUsage() {
        System.out.println("usage: " + TxSerializer.class.getName() + " -s <json_path> <txn_path> | -d <txn_path>");
    }
}
