/*
 * Copyright (c) 2009-2013, Architector Inc., Japan
 * All rights reserved.
 * 
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

package at.pkgs.nestedtx;

import java.util.logging.Logger;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import javax.sql.DataSource;

public class NestedTransactionalDataSource implements DataSource {

	public class Moderator {

		public static final int ABORTED_DEPTH = (-2);

		public static final int INITIAL_DEPTH = (-1);

		public static final int ORIGINAL_DEPTH = 0;

		private long threadId;

		private int depth;

		private NestedTransactionalConnection connection;

		private Moderator() throws SQLException {
			Connection connection;

			this.threadId = Thread.currentThread().getId();
			connection = NestedTransactionalDataSource.this.getDataSource().getConnection();
			if (!connection.getAutoCommit()) throw new SQLException("Initial auto-commit mode is false");
			connection.setAutoCommit(false);
			this.depth = Moderator.INITIAL_DEPTH;
			this.connection = new NestedTransactionalConnection(connection);
		}

		public void ensureOriginalThread() throws SQLException {
			if (this.threadId != Thread.currentThread().getId()) throw new SQLException("Invalid thread context: Not original thread");
		}

		private NestedTransactionalConnection getConnection() throws SQLException {
			this.ensureOriginalThread();
			if (this.depth >= Moderator.ORIGINAL_DEPTH) {
				return this.connection;
			}
			else {
				throw new SQLException("Invalid operation: get connection from dead transaction");
			}
		}

		public int getDepth() {
			return this.depth;
		}

		private Moderator begin() throws SQLException {
			this.ensureOriginalThread();
			if (this.depth >= Moderator.INITIAL_DEPTH) {
				this.depth ++;
			}
			else {
				throw new SQLException("Invalid operation: begin dead transaction");
			}
			return this;
		}

		public void commit() throws SQLException {
			this.ensureOriginalThread();
			if (this.depth == Moderator.ORIGINAL_DEPTH) {
				this.connection.getConnection().commit();
				this.connection.getConnection().setAutoCommit(true);
				this.connection.getConnection().close();
				NestedTransactionalDataSource.this.endTransaction();
				this.depth = Moderator.INITIAL_DEPTH;
			}
			else if (this.depth > Moderator.ORIGINAL_DEPTH) {
				depth --;
			}
			else {
				throw new SQLException("Invalid operation: commit dead transaction");
			}
		}

		public void ensureCommitted() throws SQLException {
			if (this.depth != Moderator.INITIAL_DEPTH) throw new SQLException("Not committed");
		}

		public void rollback() throws SQLException {
			try {
				if (this.depth >= Moderator.ORIGINAL_DEPTH) {
					this.ensureOriginalThread();
					this.connection.getConnection().rollback();
					this.connection.getConnection().setAutoCommit(true);
					this.connection.getConnection().close();
					this.depth = Moderator.ABORTED_DEPTH;
				}
			}
			finally {
				NestedTransactionalDataSource.this.endTransaction();
			}
		}

	}

	private ThreadLocal<Moderator> moderator;

	private DataSource source;

	public NestedTransactionalDataSource(DataSource source) {
		this.moderator = new ThreadLocal<Moderator>();
		this.source = source;
	}

	private DataSource getDataSource() {
		return this.source;
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return this.source.getLogWriter();
	}

	@Override
	public void setLogWriter(PrintWriter writer) throws SQLException {
		this.source.setLogWriter(writer);
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		return this.source.getLoginTimeout();
	}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
		this.source.setLoginTimeout(seconds);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		if (iface.isAssignableFrom(this.getClass())) {
			return true;
		}
		else if (iface.isAssignableFrom(this.source.getClass())) {
			return true;
		}
		else {
			return this.source.isWrapperFor(iface);
		}
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isAssignableFrom(this.getClass())) {
			return iface.cast(this);
		}
		else if (iface.isAssignableFrom(this.source.getClass())) {
			return iface.cast(this.source);
		}
		else {
			return this.source.unwrap(iface);
		}
	}

	@Override
	public Connection getConnection() throws SQLException {
		if (this.moderator.get() == null) {
			return this.source.getConnection();
		}
		else {
			return this.moderator.get().getConnection();
		}
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		throw new UnsupportedOperationException("Not supported by NestedTransactionalDataSource");
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return this.source.getParentLogger();
	}

	public Transaction beginTransaction() throws SQLException {
		if (this.moderator.get() == null) {
			this.moderator.set(new Moderator());
		}
		return new Transaction(this.moderator.get().begin());
	}

	private void endTransaction() {
		this.moderator.remove();
	}

}
