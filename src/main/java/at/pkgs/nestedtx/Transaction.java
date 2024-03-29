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

import java.sql.SQLException;

public class Transaction implements AutoCloseable {

	private final NestedTransactionalDataSource.Moderator moderator;

	private boolean closed;

	Transaction(NestedTransactionalDataSource.Moderator moderator) {
		this.moderator = moderator;
		this.closed = false;
	}

	@Override
	public String toString() {
		StringBuilder builder;

		builder = new StringBuilder();
		builder.append(super.toString());
		builder.append(": depth:");
		builder.append(this.moderator.getDepth());
		builder.append(": thread:");
		builder.append(Thread.currentThread().getId());
		return builder.toString();
	}

	public void commit() throws SQLException {
		this.moderator.commit();
		this.closed = true;
	}

	@Override
	public void close() throws SQLException {
		if (!this.closed) {
			this.moderator.rollback();
			this.closed = true;
		}
	}

}
