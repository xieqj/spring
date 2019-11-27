/**
 *    Copyright 2010-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.mybatis.spring;

import static org.springframework.util.Assert.notNull;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.transaction.support.ResourceHolderSupport;

/**
 * Used to keep current {@code SqlSession} in {@code TransactionSynchronizationManager}.
 * The {@code SqlSessionFactory} that created that {@code SqlSession} is used as a key.
 * {@code ExecutorType} is also kept to be able to check if the user is trying to change it
 * during a TX (that is not allowed) and throw a Exception in that case.
 *
 * @author Hunter Presnall
 * @author Eduardo Macarron
 */
public final class SqlSessionHolder extends ResourceHolderSupport {

	private final SqlSession sqlSession;

	private final ExecutorType executorType;

	private final PersistenceExceptionTranslator exceptionTranslator;
	private ConcurrentHashMap<ExecutorType, SqlSession> sessions;

	/**
	 * Creates a new holder instance.
	 *
	 * @param sqlSession the {@code SqlSession} has to be hold.
	 * @param executorType the {@code ExecutorType} has to be hold.
	 */
	public SqlSessionHolder(SqlSession sqlSession,
			ExecutorType executorType,
			PersistenceExceptionTranslator exceptionTranslator) {

		notNull(sqlSession, "SqlSession must not be null");
		notNull(executorType, "ExecutorType must not be null");

		this.sqlSession = sqlSession;
		this.executorType = executorType;
		this.exceptionTranslator = exceptionTranslator;
	}

	public SqlSession getSqlSession() {
		return sqlSession;
	}

	public SqlSession getSqlSession(ExecutorType executorType) {
		if(this.executorType==executorType){
			return this.sqlSession;
		}
		if(this.sessions==null){
			return null;
		}
		return this.sessions.get(executorType);
	}

	public void addSqlSession(ExecutorType executorType, SqlSession session){
		if(executorType!=this.executorType){
			if(this.sessions==null){
				this.sessions=new ConcurrentHashMap<ExecutorType, SqlSession>();
				this.sessions.put(executorType, session);
			}else if(!this.sessions.containsKey(executorType)){
				this.sessions.put(executorType, session);
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder sb=new StringBuilder();
		sb.append("default:").append(this.getTypeName(this.executorType)).append(":");
		sb.append(this.sqlSession);
		if(this.sessions!=null){
			sb.append(";");
			Map.Entry<ExecutorType, SqlSession> entry;
			ExecutorType type;
			for(Iterator<Map.Entry<ExecutorType, SqlSession>> it=this.sessions.entrySet().iterator();it.hasNext();){
				entry=it.next();
				type=entry.getKey();
				sb.append(this.getTypeName(type));
				sb.append(":");
				sb.append(entry.getValue().toString());
				sb.append(";");
			}
		}
		return sb.toString();
	}

	private String getTypeName(ExecutorType type){
		if(type==ExecutorType.REUSE){
			return "REUSE";
		}else if(type==ExecutorType.BATCH){
			return "BATCH";
		}else if(type==ExecutorType.SIMPLE){
			return "SIMPLE";
		}
		return "UNKNOW";
	}

	public ExecutorType getExecutorType() {
		return executorType;
	}

	public PersistenceExceptionTranslator getPersistenceExceptionTranslator() {
		return exceptionTranslator;
	}

	public void close() {
		this.sqlSession.close();
		if(this.sessions!=null){
			for(Iterator<SqlSession> it=this.sessions.values().iterator();it.hasNext();){
				it.next().close();
			}
		}

	}

	public void commit() {
		this.sqlSession.commit();
		if(this.sessions!=null){
			for(Iterator<SqlSession> it=this.sessions.values().iterator();it.hasNext();){
				it.next().commit();
			}
		}

	}

	public boolean isHolded(SqlSession session) {
		if(this.sqlSession==session){
			return true;
		}
		if(this.sessions!=null){
			for(Iterator<SqlSession> it=this.sessions.values().iterator();it.hasNext();){
				if(it.next()==session){
					return true;
				}
			}
		}
		return false;
	}
}
