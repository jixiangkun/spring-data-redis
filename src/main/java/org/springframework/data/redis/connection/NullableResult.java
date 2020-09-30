/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.function.Function;

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 */
public class NullableResult<T> {

	private static final NullableResult EMPTY = new NullableResult(null);

	@Nullable private final T value;

	private NullableResult(@Nullable T value) {
		this.value = value;
	}

	public static <T> NullableResult<T> of(@Nullable T value) {

		if (value == null) {
			return EMPTY;
		}

		return new NullableResult<>(value);
	}

	public <S> NullableResult<S> map(Function<T, S> function) {
		return value == null ? EMPTY : of(function.apply(value));
	}

	public <S> NullableResult<S> convert(Converter<T, S> converter) {
		return map(converter::convert);
	}

	@Nullable
	public T get() {
		return value;
	}

	public T getOrDefault(T defaultValue) {
		return value == null ? defaultValue : value;
	}
}
