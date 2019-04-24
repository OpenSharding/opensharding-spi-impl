/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.shardingsphere.transaction.saga.persistence.impl.jdbc;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import org.apache.shardingsphere.core.exception.ShardingException;

/**
 * SQL file reader.
 *
 * @author yangyi
 */
public class SQLFileReader {
    
    private static final String SQL_FILE = "schema-init.sql";
    
    private static final String DELIMITER = ";";
    
    /**
     * read SQLs from SQL file.
     *
     * @return collection of sql
     */
    public static Collection<String> readSQLs() {
        Optional<BufferedReader> lineReader = getLineReader();
        return lineReader.isPresent() ? readSQLsFromFile(lineReader.get()) : Lists.<String>newArrayList();
    }
    
    private static Optional<BufferedReader> getLineReader() {
        URL sqlFile = SQLFileReader.class.getClassLoader().getResource(SQL_FILE);
        if (null == sqlFile) {
            return Optional.absent();
        }
        try {
            return Optional.of(new BufferedReader(new FileReader(sqlFile.getFile())));
        } catch (final FileNotFoundException ex) {
            return Optional.absent();
        }
    }
    
    private static Collection<String> readSQLsFromFile(final BufferedReader lineReader) {
        List<String> result = Lists.newArrayList();
        Optional<String> sql;
        try (BufferedReader needCloseReader = lineReader) {
            while ((sql = nextSQL(needCloseReader)).isPresent()) {
                result.add(sql.get());
            }
        } catch (final IOException ex) {
            throw new ShardingException("read SQLs failed", ex);
        }
        return result;
    }
    
    private static Optional<String> nextSQL(final BufferedReader lineReader) throws IOException {
        StringBuilder result = new StringBuilder();
        String line = "";
        while (null != (line = lineReader.readLine())) {
            line = line.trim();
            if (line.length() > 0 && lineNotComment(line)) {
                result.append(line);
                if (line.trim().endsWith(DELIMITER)) {
                    return Optional.of(result.substring(0, result.length() - 1));
                }
            }
        }
        if (result.length() > 0) {
            return Optional.of(result.toString());
        }
        return Optional.absent();
    }
    
    private static boolean lineNotComment(final String line) {
        return !(line.startsWith("#") || line.startsWith("//") || line.startsWith("--"));
    }
}
