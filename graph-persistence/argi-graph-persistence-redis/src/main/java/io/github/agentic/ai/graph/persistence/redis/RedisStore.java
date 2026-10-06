/*
 * Copyright 2024-2026 the original author or authors.
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
package io.github.agentic.ai.graph.persistence.redis;

import io.github.agentic.ai.graph.store.NamespaceListRequest;
import io.github.agentic.ai.graph.store.StoreItem;
import io.github.agentic.ai.graph.store.StoreSearchRequest;
import io.github.agentic.ai.graph.store.StoreSearchResult;
import io.github.agentic.ai.graph.store.VersionedStore;
import io.github.agentic.ai.graph.store.VersionedStoreItem;
import io.github.agentic.ai.graph.store.stores.BaseStore;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import static java.util.Objects.requireNonNull;

/**
 * Redis-backed Store implementation.
 */
public class RedisStore extends BaseStore implements VersionedStore {

	public static final String DEFAULT_STORAGE_KEY = "argi:store:items:v1";

	private static final int MAX_MUTATION_ATTEMPTS = 64;

	private final RMap<String, String> items;

	private final ObjectMapper objectMapper;

	public RedisStore(RedissonClient redisson) {
		this(redisson, DEFAULT_STORAGE_KEY);
	}

	public RedisStore(RedissonClient redisson, String storageKey) {
		requireNonNull(redisson, "redisson cannot be null");
		if (storageKey == null || storageKey.isBlank()) {
			throw new IllegalArgumentException("storageKey cannot be null or blank");
		}
		this.items = redisson.getMap(storageKey, StringCodec.INSTANCE);
		this.objectMapper = new ObjectMapper();
		this.objectMapper.findAndRegisterModules();
	}

	@Override
	public VersionedStoreItem getVersionedItem(List<String> namespace, String key) {
		validateGetItem(namespace, key);
		String raw = items.get(createStoreKey(namespace, key));
		if (raw == null) {
			return new VersionedStoreItem(Optional.empty(), 0);
		}
		StoreEnvelope envelope = deserialize(raw);
		if (envelope.deleted || envelope.item == null) {
			return new VersionedStoreItem(Optional.empty(), envelope.version);
		}
		return new VersionedStoreItem(Optional.of(envelope.item), envelope.version);
	}

	@Override
	public boolean putItemIfVersion(StoreItem item, long expectedVersion) {
		validatePutItem(item);
		validateExpectedVersion(expectedVersion);
		String field = createStoreKey(item.getNamespace(), item.getKey());
		for (int attempt = 0; attempt < MAX_MUTATION_ATTEMPTS; attempt++) {
			String current = items.get(field);
			if (current == null) {
				if (expectedVersion != 0) {
					return false;
				}
				String next = serialize(new StoreEnvelope(Math.addExact(0, 1), false, item));
				if (items.putIfAbsent(field, next) == null) {
					return true;
				}
				continue;
			}

			StoreEnvelope envelope = deserialize(current);
			if (envelope.version != expectedVersion) {
				return false;
			}
			String next = serialize(new StoreEnvelope(Math.addExact(envelope.version, 1), false, item));
			if (items.replace(field, current, next)) {
				return true;
			}
		}
		throw new IllegalStateException("Failed to update Redis Store item after " + MAX_MUTATION_ATTEMPTS
				+ " attempts: " + field);
	}

	@Override
	public boolean deleteItemIfVersion(List<String> namespace, String key, long expectedVersion) {
		validateDeleteItem(namespace, key);
		validateExpectedVersion(expectedVersion);
		String field = createStoreKey(namespace, key);
		for (int attempt = 0; attempt < MAX_MUTATION_ATTEMPTS; attempt++) {
			String current = items.get(field);
			if (current == null) {
				return false;
			}
			StoreEnvelope envelope = deserialize(current);
			if (envelope.deleted || envelope.item == null) {
				return false;
			}
			if (envelope.version != expectedVersion) {
				return false;
			}
			String next = serialize(new StoreEnvelope(Math.addExact(envelope.version, 1), true, null));
			if (items.replace(field, current, next)) {
				return true;
			}
		}
		throw new IllegalStateException("Failed to delete Redis Store item after " + MAX_MUTATION_ATTEMPTS
				+ " attempts: " + field);
	}

	@Override
	public void putItem(StoreItem item) {
		validatePutItem(item);
		String field = createStoreKey(item.getNamespace(), item.getKey());
		for (int attempt = 0; attempt < MAX_MUTATION_ATTEMPTS; attempt++) {
			String current = items.get(field);
			if (current == null) {
				String next = serialize(new StoreEnvelope(Math.addExact(0, 1), false, item));
				if (items.putIfAbsent(field, next) == null) {
					return;
				}
				continue;
			}
			StoreEnvelope envelope = deserialize(current);
			String next = serialize(new StoreEnvelope(Math.addExact(envelope.version, 1), false, item));
			if (items.replace(field, current, next)) {
				return;
			}
		}
		throw new IllegalStateException("Failed to store Redis Store item after " + MAX_MUTATION_ATTEMPTS
				+ " attempts: " + field);
	}

	@Override
	public Optional<StoreItem> getItem(List<String> namespace, String key) {
		return getVersionedItem(namespace, key).item();
	}

	@Override
	public boolean deleteItem(List<String> namespace, String key) {
		validateDeleteItem(namespace, key);
		String field = createStoreKey(namespace, key);
		for (int attempt = 0; attempt < MAX_MUTATION_ATTEMPTS; attempt++) {
			String current = items.get(field);
			if (current == null) {
				return false;
			}
			StoreEnvelope envelope = deserialize(current);
			if (envelope.deleted || envelope.item == null) {
				return false;
			}
			String next = serialize(new StoreEnvelope(Math.addExact(envelope.version, 1), true, null));
			if (items.replace(field, current, next)) {
				return true;
			}
		}
		throw new IllegalStateException("Failed to delete Redis Store item after " + MAX_MUTATION_ATTEMPTS
				+ " attempts: " + field);
	}

	@Override
	public StoreSearchResult searchItems(StoreSearchRequest searchRequest) {
		validateSearchItems(searchRequest);
		List<StoreItem> filteredItems = getAllItems().stream()
			.filter(item -> matchesSearchCriteria(item, searchRequest))
			.collect(Collectors.toList());

		if (!searchRequest.getSortFields().isEmpty()) {
			filteredItems.sort(createComparator(searchRequest));
		}

		long totalCount = filteredItems.size();
		int offset = searchRequest.getOffset();
		int limit = searchRequest.getLimit();
		if (offset >= filteredItems.size()) {
			return StoreSearchResult.of(Collections.emptyList(), totalCount, offset, limit);
		}
		int endIndex = Math.min(offset + limit, filteredItems.size());
		return StoreSearchResult.of(new ArrayList<>(filteredItems.subList(offset, endIndex)), totalCount, offset, limit);
	}

	@Override
	public List<String> listNamespaces(NamespaceListRequest namespaceRequest) {
		validateListNamespaces(namespaceRequest);
		Set<String> namespaceSet = new HashSet<>();
		List<String> prefixFilter = namespaceRequest.getNamespace();
		for (StoreItem item : getAllItems()) {
			List<String> itemNamespace = item.getNamespace();
			if (!startsWithPrefix(itemNamespace, prefixFilter)) {
				continue;
			}
			int maxDepth = namespaceRequest.getMaxDepth();
			int depth = (maxDepth == -1) ? itemNamespace.size() : Math.min(maxDepth, itemNamespace.size());
			int startDepth = prefixFilter.isEmpty() ? 1 : prefixFilter.size();
			for (int i = startDepth; i <= depth; i++) {
				namespaceSet.add(String.join("/", itemNamespace.subList(0, i)));
			}
		}
		List<String> namespaces = new ArrayList<>(namespaceSet);
		Collections.sort(namespaces);
		int offset = namespaceRequest.getOffset();
		int limit = namespaceRequest.getLimit();
		if (offset >= namespaces.size()) {
			return Collections.emptyList();
		}
		int endIndex = Math.min(offset + limit, namespaces.size());
		return namespaces.subList(offset, endIndex);
	}

	@Override
	public void clear() {
		for (Map.Entry<String, String> entry : items.readAllEntrySet()) {
			clearLiveEntry(entry.getKey());
		}
	}

	@Override
	public long size() {
		return getAllItems().size();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	private void clearLiveEntry(String field) {
		for (int attempt = 0; attempt < MAX_MUTATION_ATTEMPTS; attempt++) {
			String current = items.get(field);
			if (current == null) {
				return;
			}
			StoreEnvelope envelope = deserialize(current);
			if (envelope.deleted || envelope.item == null) {
				return;
			}
			String next = serialize(new StoreEnvelope(Math.addExact(envelope.version, 1), true, null));
			if (items.replace(field, current, next)) {
				return;
			}
		}
		throw new IllegalStateException("Failed to clear Redis Store item after " + MAX_MUTATION_ATTEMPTS
				+ " attempts: " + field);
	}

	private List<StoreItem> getAllItems() {
		List<StoreItem> liveItems = new ArrayList<>();
		for (String raw : items.readAllValues()) {
			StoreEnvelope envelope = deserialize(raw);
			if (!envelope.deleted && envelope.item != null) {
				liveItems.add(envelope.item);
			}
		}
		return liveItems;
	}

	private void validateExpectedVersion(long expectedVersion) {
		if (expectedVersion < 0) {
			throw new IllegalArgumentException("expectedVersion cannot be negative");
		}
	}

	private String serialize(StoreEnvelope envelope) {
		try {
			return objectMapper.writeValueAsString(envelope);
		}
		catch (Exception ex) {
			throw new RuntimeException("Failed to serialize Redis Store envelope", ex);
		}
	}

	private StoreEnvelope deserialize(String raw) {
		try {
			return objectMapper.readValue(raw, StoreEnvelope.class);
		}
		catch (Exception ex) {
			throw new RuntimeException("Failed to deserialize Redis Store envelope", ex);
		}
	}

	private static final class StoreEnvelope {

		public long version;

		public boolean deleted;

		public StoreItem item;

		@SuppressWarnings("unused")
		private StoreEnvelope() {
		}

		private StoreEnvelope(long version, boolean deleted, StoreItem item) {
			this.version = version;
			this.deleted = deleted;
			this.item = item;
		}

	}

}
