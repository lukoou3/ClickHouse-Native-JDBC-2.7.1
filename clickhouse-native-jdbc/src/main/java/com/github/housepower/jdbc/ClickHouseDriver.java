/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.housepower.jdbc;

import com.github.housepower.settings.ClickHouseConfig;
import com.github.housepower.settings.ClickHouseDefines;
import com.github.housepower.settings.SettingKey;

import java.io.Serializable;
import java.sql.*;
import java.util.Map;
import java.util.Properties;

/**
 * jdbc driver: implements java.sql.Driver 实现规范即可
 *
 */
public class ClickHouseDriver implements Driver {

    static {
        try {
            // 注册driver
            DriverManager.registerDriver(new ClickHouseDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        // startsWith "jdbc:clickhouse:"
        return url.startsWith(ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX);
    }

    @Override
    public ClickHouseConnection connect(String url, Properties properties) throws SQLException {
        if (!this.acceptsURL(url)) {
            return null;
        }

        // 构造cfg
        ClickHouseConfig cfg = ClickHouseConfig.Builder.builder()
                .withJdbcUrl(url) // 解析出url里的settings
                .withProperties(properties) // 合并properties里的settings
                .build();
        // 返回ClickHouseConnection
        return connect(url, cfg);
    }

    ClickHouseConnection connect(String url, ClickHouseConfig cfg) throws SQLException {
        if (!this.acceptsURL(url)) {
            return null;
        }
        return ClickHouseConnection.createClickHouseConnection(cfg.withJdbcUrl(url));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) throws SQLException {
        ClickHouseConfig cfg = ClickHouseConfig.Builder.builder()
                .withJdbcUrl(url)
                .withProperties(properties)
                .build();
        int index = 0;
        DriverPropertyInfo[] driverPropertiesInfo = new DriverPropertyInfo[cfg.settings().size()];

        for (Map.Entry<SettingKey, Serializable> entry : cfg.settings().entrySet()) {
            String value = String.valueOf(entry.getValue());

            DriverPropertyInfo property = new DriverPropertyInfo(entry.getKey().name(), value);
            property.description = entry.getKey().description();

            driverPropertiesInfo[index++] = property;
        }

        return driverPropertiesInfo;
    }

    @Override
    public int getMajorVersion() {
        return ClickHouseDefines.MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return ClickHouseDefines.MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        // 是否符合JDBC规范
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
