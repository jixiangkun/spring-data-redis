/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;

/**
 * @author Mark Paluch
 */
class LettuceInvoker {

	private final RedisClusterAsyncCommands<byte[], byte[]> connection;
	private final Synchronizer synchronizer;

	public LettuceInvoker(RedisClusterAsyncCommands<byte[], byte[]> connection, Synchronizer synchronizer) {
		this.connection = connection;
		this.synchronizer = synchronizer;
	}

	@Nullable
	<R> R just(ConnectionFunction0<R> function) {
		return synchronizer.invoke(function.apply(connection), source -> source);
	}

	@Nullable
	<R, T1> R just(ConnectionFunction1<T1, R> function, T1 t1) {
		return just(it -> function.apply(it, t1));
	}

	@Nullable
	<R, T1, T2> R just(ConnectionFunction2<T1, T2, R> function, T1 t1, T2 t2) {
		return just(it -> function.apply(it, t1, t2));
	}

	@Nullable
	<R, T1, T2, T3> R just(ConnectionFunction3<T1, T2, T3, R> function, T1 t1, T2 t2, T3 t3) {
		return just(it -> function.apply(it, t1, t2, t3));
	}

	@Nullable
	<R, T1, T2, T3, T4> R just(ConnectionFunction4<T1, T2, T3, T4, R> function, T1 t1, T2 t2, T3 t3, T4 t4) {
		return just(it -> function.apply(it, t1, t2, t3, t4));
	}

	@Nullable
	<R, T1, T2, T3, T4, T5> R just(ConnectionFunction5<T1, T2, T3, T4, T5, R> function, T1 t1, T2 t2, T3 t3, T4 t4,
			T5 t5) {
		return just(it -> function.apply(it, t1, t2, t3, t4, t5));
	}

	<R> SingleInvocationSpec<R> from(ConnectionFunction0<R> function) {
		return new DefaultSingleInvocationSpec<>(() -> function.apply(connection), synchronizer);
	}

	<R, T1> SingleInvocationSpec<R> from(ConnectionFunction1<T1, R> function, T1 t1) {
		return from(it -> function.apply(it, t1));
	}

	<R, T1, T2> SingleInvocationSpec<R> from(ConnectionFunction2<T1, T2, R> function, T1 t1, T2 t2) {
		return from(it -> function.apply(it, t1, t2));
	}

	<R, T1, T2, T3> SingleInvocationSpec<R> from(ConnectionFunction3<T1, T2, T3, R> function, T1 t1, T2 t2, T3 t3) {
		return from(it -> function.apply(it, t1, t2, t3));
	}

	<R, T1, T2, T3, T4> SingleInvocationSpec<R> from(ConnectionFunction4<T1, T2, T3, T4, R> function, T1 t1, T2 t2, T3 t3,
			T4 t4) {
		return from(it -> function.apply(it, t1, t2, t3, t4));
	}

	<R, T1, T2, T3, T4, T5> SingleInvocationSpec<R> from(ConnectionFunction5<T1, T2, T3, T4, T5, R> function, T1 t1,
			T2 t2, T3 t3, T4 t4, T5 t5) {
		return from(it -> function.apply(it, t1, t2, t3, t4, t5));
	}

	<R extends Collection<E>, E> ManyInvocationSpec<E> fromMany(ConnectionFunction0<R> function) {
		return new DefaultManyInvocationSpec<>(() -> function.apply(connection), synchronizer);
	}

	<R extends Collection<E>, E, T1> ManyInvocationSpec<E> fromMany(ConnectionFunction1<T1, R> function, T1 t1) {
		return fromMany(it -> function.apply(it, t1));
	}

	<R extends Collection<E>, E, T1, T2> ManyInvocationSpec<E> fromMany(ConnectionFunction2<T1, T2, R> function, T1 t1,
			T2 t2) {
		return fromMany(it -> function.apply(it, t1, t2));
	}

	<R extends Collection<E>, E, T1, T2, T3> ManyInvocationSpec<E> fromMany(ConnectionFunction3<T1, T2, T3, R> function,
			T1 t1, T2 t2, T3 t3) {
		return fromMany(it -> function.apply(it, t1, t2, t3));
	}

	<R extends Collection<E>, E, T1, T2, T3, T4> ManyInvocationSpec<E> fromMany(
			ConnectionFunction4<T1, T2, T3, T4, R> function, T1 t1, T2 t2, T3 t3, T4 t4) {
		return fromMany(it -> function.apply(it, t1, t2, t3, t4));
	}

	<R extends Collection<E>, E, T1, T2, T3, T4, T5> ManyInvocationSpec<E> fromMany(
			ConnectionFunction5<T1, T2, T3, T4, T5, R> function, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
		return fromMany(it -> function.apply(it, t1, t2, t3, t4, t5));
	}

	public interface SingleInvocationSpec<S> {

		@Nullable
		<T> T get(Converter<S, T> converter);
	}

	public interface ManyInvocationSpec<S> {

		<T> List<T> toList(Converter<S, T> converter);

		<T> Set<T> toSet(Converter<S, T> converter);
	}

	public interface ConnectionFunction0<R> {

		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection);
	}

	public interface ConnectionFunction1<T1, R> {

		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1);
	}

	public interface ConnectionFunction2<T1, T2, R> {

		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1, T2 t2);
	}

	public interface ConnectionFunction3<T1, T2, T3, R> {

		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1, T2 t2, T3 t3);
	}

	public interface ConnectionFunction4<T1, T2, T3, T4, R> {

		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1, T2 t2, T3 t3, T4 t4);
	}

	public interface ConnectionFunction5<T1, T2, T3, T4, T5, R> {

		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5);
	}

	public interface ConnectionFunction6<T1, T2, T3, T4, T5, T6, R> {

		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5,
				T6 t6);
	}

	public interface ConnectionFunction7<T1, T2, T3, T4, T5, T6, T7, R> {

		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6,
				T7 t7);
	}

	public interface ConnectionFunction8<T1, T2, T3, T4, T5, T6, T7, T8, R> {

		RedisFuture<R> apply(RedisClusterAsyncCommands<byte[], byte[]> connection, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6,
				T7 t7, T8 t8);
	}

	static class DefaultSingleInvocationSpec<S> implements SingleInvocationSpec<S> {

		private final Supplier<RedisFuture<S>> parent;
		private final Synchronizer synchronizer;

		public DefaultSingleInvocationSpec(Supplier<RedisFuture<S>> parent, Synchronizer synchronizer) {
			this.parent = parent;
			this.synchronizer = synchronizer;
		}

		@Override
		public <T> T get(Converter<S, T> converter) {
			return synchronizer.invoke(parent.get(), converter);
		}
	}

	static class DefaultManyInvocationSpec<S> implements ManyInvocationSpec<S> {

		private final Supplier<RedisFuture<? extends Collection<S>>> parent;
		private final Synchronizer synchronizer;

		public DefaultManyInvocationSpec(Supplier<RedisFuture<? extends Collection<S>>> parent, Synchronizer synchronizer) {
			this.parent = parent;
			this.synchronizer = synchronizer;
		}

		@Override
		public <T> List<T> toList(Converter<S, T> converter) {

			return synchronizer.invoke(parent.get(), source -> {

				if (source == null || source.isEmpty()) {
					return Collections.emptyList();
				}

				List<T> result = new ArrayList<>(source.size());

				for (S s : source) {
					result.add(converter.convert(s));
				}

				return result;
			});
		}

		@Override
		public <T> Set<T> toSet(Converter<S, T> converter) {

			return synchronizer.invoke(parent.get(), source -> {

				if (source == null || source.isEmpty()) {
					return Collections.emptySet();
				}

				Set<T> result = new LinkedHashSet<>(source.size());

				for (S s : source) {
					result.add(converter.convert(s));
				}

				return result;
			});
		}
	}

	interface Synchronizer {

		@Nullable
		@SuppressWarnings({ "unchecked", "rawtypes" })
		default <I, T> T invoke(RedisFuture<I> future, Converter<I, T> converter) {
			return (T) doInvoke((RedisFuture) future, (Converter<Object, Object>) converter);
		}

		@Nullable
		Object doInvoke(RedisFuture<Object> future, Converter<Object, Object> converter);

	}
}
