/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.gcloud.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.services.datastore.client.DatastoreException;
import com.google.gcloud.datastore.Query.Type;
import com.google.gcloud.datastore.StructuredQuery.OrderBy;
import com.google.gcloud.datastore.StructuredQuery.Projection;
import com.google.gcloud.datastore.StructuredQuery.PropertyFilter;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@RunWith(JUnit4.class)
public class DatastoreServiceTest {

  private static final String DATASET = LocalGcdHelper.DEFAULT_DATASET;
  private static final String KIND1 = "kind1";
  private static final String KIND2 = "kind2";
  private static final String KIND3 = "kind3";
  private static final NullValue NULL_VALUE = NullValue.of();
  private static final StringValue STR_VALUE = StringValue.of("str");
  private static final BooleanValue BOOL_VALUE = BooleanValue.builder(false).indexed(false).build();
  private static final PartialKey PARTIAL_KEY1 = PartialKey.builder(DATASET, KIND1).build();
  private static final PartialKey PARTIAL_KEY2 = PartialKey.builder(DATASET, KIND2).build();
  private static final Key KEY1 = Key.builder(PARTIAL_KEY1, "name").build();
  private static final Key KEY2 = Key.builder(KEY1, KIND2, 1).build();
  private static final Key KEY3 = Key.builder(KEY2).name("bla").build();
  private static final Key KEY4 = Key.builder(KEY2).name("newName1").build();
  private static final Key KEY5 = Key.builder(KEY2).name("newName2").build();
  private static final KeyValue KEY_VALUE = KeyValue.of(KEY1);
  private static final ListValue LIST_VALUE1 = ListValue.builder()
      .addValue(NULL_VALUE)
      .addValue(STR_VALUE, BOOL_VALUE)
      .build();
  private static final ListValue LIST_VALUE2 = ListValue.of(Collections.singletonList(KEY_VALUE));
  private static final DateTimeValue DATE_TIME_VALUE = new DateTimeValue(DateTime.now());
  private static final PartialEntity PARTIAL_ENTITY1 = PartialEntity.builder(PARTIAL_KEY2)
      .set("str", STR_VALUE).set("bool", BOOL_VALUE).set("list", LIST_VALUE1).build();
  private static final PartialEntity PARTIAL_ENTITY2 = PartialEntity.builder(PARTIAL_ENTITY1)
      .remove("str").set("bool", true).set("list", LIST_VALUE1.get()).build();
  private static final PartialEntity PARTIAL_ENTITY3 = PartialEntity.builder(PARTIAL_ENTITY1)
      .key(PartialKey.builder(DATASET, KIND3).build()).build();
  private static final Entity ENTITY1 = Entity.builder(KEY1)
      .set("str", STR_VALUE)
      .set("date", DATE_TIME_VALUE)
      .set("bool", BOOL_VALUE)
      .set("partial1", EntityValue.of(PARTIAL_ENTITY1))
      .set("list", LIST_VALUE2)
      .build();
  private static final Entity ENTITY2 = Entity.builder(ENTITY1).key(KEY2).remove("str")
      .set("name", "Dan").setNull("null").set("age", 20).build();
  private static final Entity ENTITY3 = Entity.builder(ENTITY1).key(KEY3).remove("str")
      .set("null", NULL_VALUE).set("partial1", PARTIAL_ENTITY2).set("partial2", ENTITY2).build();

  private DatastoreServiceOptions options;
  private DatastoreService datastore;
  private DatastoreHelper helper;

  private static LocalGcdHelper gcdHelper;

  @BeforeClass
  public static void beforeClass() throws IOException, InterruptedException {
    if (!LocalGcdHelper.isActive(DATASET)) {
      gcdHelper = LocalGcdHelper.start(DATASET);
    }
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    options = DatastoreServiceOptions.builder()
        .dataset(DATASET)
        .host("http://localhost:" + LocalGcdHelper.PORT)
        .build();
    datastore = DatastoreServiceFactory.getDefault(options);
    helper = DatastoreHelper.createFor(datastore);
    // Prepare data for testing
    datastore.delete(KEY1, KEY2, KEY3, KEY4, KEY5);
    datastore.add(ENTITY1, ENTITY2);
  }

  @AfterClass
  public static void afterClass() throws IOException, InterruptedException {
    if (gcdHelper != null) {
      gcdHelper.stop();
    }
  }

  @Test
  public void testGetOptions() {
    assertSame(options, datastore.options());
  }

  @Test
  public void testNewTransactionCommit() {
    Transaction transaction = datastore.newTransaction();
    transaction.add(ENTITY3);
    Entity entity2 = Entity.builder(ENTITY2)
        .clear()
        .setNull("bla")
        .build();
    transaction.update(entity2);
    transaction.delete(KEY1);
    transaction.commit();

    List<Entity> list = helper.fetch(KEY1, KEY2, KEY3);
    assertNull(list.get(0));
    assertEquals(entity2, list.get(1));
    assertEquals(ENTITY3, list.get(2));
    assertEquals(3, list.size());

    try {
      transaction.commit();
      fail("Expecting a failure");
    } catch (DatastoreServiceException ex) {
      // expected to fail
    }

    try {
      transaction.rollback();
      fail("Expecting a failure");
    } catch (DatastoreServiceException ex) {
      // expected to fail
    }

    verifyNotUsable(transaction);
  }

  @Test
  public void testTransactionWithRead() {
    Transaction transaction = datastore.newTransaction();
    assertNull(transaction.get(KEY3));
    transaction.add(ENTITY3);
    transaction.commit();
    assertEquals(ENTITY3, datastore.get(KEY3));

    transaction = datastore.newTransaction();
    assertEquals(ENTITY3, transaction.get(KEY3));
    // update entity3 during the transaction
    datastore.put(Entity.builder(ENTITY3).clear().build());
    transaction.update(ENTITY2);
    try {
      transaction.commit();
      fail("Expecting a failure");
    } catch (DatastoreServiceException expected) {
      assertEquals(DatastoreServiceException.Code.ABORTED, expected.code());
    }
  }

  @Test
  public void testTransactionWithQuery() {
    Query<Entity> query =
        StructuredQuery.builder().kind(KIND2).filter(PropertyFilter.hasAncestor(KEY2)).build();
    Transaction transaction = datastore.newTransaction();
    QueryResult<Entity> results = transaction.run(query);
    assertEquals(ENTITY2, results.next());
    assertFalse(results.hasNext());
    transaction.add(ENTITY3);
    transaction.commit();
    assertEquals(ENTITY3, datastore.get(KEY3));

    transaction = datastore.newTransaction();
    results = transaction.run(query);
    assertEquals(ENTITY2, results.next());
    transaction.delete(ENTITY3.key());
    // update entity2 during the transaction
    datastore.put(Entity.builder(ENTITY2).clear().build());
    try {
      transaction.commit();
      fail("Expecting a failure");
    } catch (DatastoreServiceException expected) {
      assertEquals(DatastoreServiceException.Code.ABORTED, expected.code());
    }
  }

  @Test
  public void testNewTransactionRollback() {
    Transaction transaction = datastore.newTransaction();
    transaction.add(ENTITY3);
    Entity entity2 = Entity.builder(ENTITY2).clear().setNull("bla")
        .set("list3", StringValue.of("bla"), StringValue.builder("bla").build()).build();
    transaction.update(entity2);
    transaction.delete(KEY1);
    transaction.rollback();
    transaction.rollback(); // should be safe to repeat rollback calls

    try {
      transaction.commit();
      fail("Expecting a failure");
    } catch (DatastoreServiceException ex) {
      // expected to fail
    }

    verifyNotUsable(transaction);

    List<Entity> list = helper.fetch(KEY1, KEY2, KEY3);
    assertEquals(ENTITY1, list.get(0));
    assertEquals(ENTITY2, list.get(1));
    assertNull(list.get(2));
    assertEquals(3, list.size());
  }

  private void verifyNotUsable(DatastoreWriter writer) {
    try {
      writer.add(ENTITY3);
      fail("Expecting a failure");
    } catch (DatastoreServiceException ex) {
      // expected to fail
    }

    try {
      writer.put(ENTITY3);
      fail("Expecting a failure");
    } catch (DatastoreServiceException ex) {
      // expected to fail
    }

    try {
      writer.update(ENTITY3);
      fail("Expecting a failure");
    } catch (DatastoreServiceException ex) {
      // expected to fail
    }

    try {
      writer.delete(ENTITY3.key());
      fail("Expecting a failure");
    } catch (DatastoreServiceException ex) {
      // expected to fail
    }
  }

  @Test
  public void testNewBatch() {
    Batch batch = datastore.newBatch();
    Entity entity1 = Entity.builder(ENTITY1).clear().build();
    Entity entity2 = Entity.builder(ENTITY2).clear().setNull("bla").build();
    Entity entity4 = Entity.builder(KEY4).set("value", StringValue.of("value")).build();
    Entity entity5 = Entity.builder(KEY5).set("value", "value").build();

    batch.add(entity4, entity5);
    batch.add(PARTIAL_ENTITY3);
    batch.put(ENTITY3, entity1, entity2);

    Batch.Response response = batch.submit();
    Iterator<Entity> entities =
        helper.fetch(KEY1, KEY2, KEY3, entity4.key(), entity5.key()).iterator();
    assertEquals(entity1, entities.next());
    assertEquals(entity2, entities.next());
    assertEquals(ENTITY3, entities.next());
    assertEquals(entity4, entities.next());
    assertEquals(entity5, entities.next());
    assertFalse(entities.hasNext());
    List<Key> generatedKeys = response.generatedKeys();
    assertEquals(1, generatedKeys.size());
    assertEquals(PARTIAL_ENTITY1.properties(), datastore.get(generatedKeys.get(0)).properties());

    try {
      batch.submit();
      fail("Expecting a failure");
    } catch (DatastoreServiceException ex) {
      // expected to fail
    }
    verifyNotUsable(batch);

    batch = datastore.newBatch();
    batch.delete(entity4.key(), entity5.key());
    batch.update(ENTITY1, ENTITY2, ENTITY3);
    batch.submit();
    entities = helper.fetch(KEY1, KEY2, KEY3, entity4.key(), entity5.key()).iterator();
    assertEquals(ENTITY1, entities.next());
    assertEquals(ENTITY2, entities.next());
    assertEquals(ENTITY3, entities.next());
    assertNull(entities.next());
    assertNull(entities.next());
    assertFalse(entities.hasNext());

    // TODO need to cover the following use-cases:
    // delete after put/add/update
    // put after delete/add/update
    // update after delete/add/put
    // add after delete/update/put
  }

  @Test
  public void testRunGqlQueryNoCasting() throws DatastoreException {
    Query<Entity> query1 = GqlQuery.builder(Type.FULL, "select * from " + KIND1).build();
    QueryResult<Entity> results1 = datastore.run(query1);
    assertTrue(results1.hasNext());
    assertEquals(ENTITY1, results1.next());
    assertFalse(results1.hasNext());

    datastore.put(ENTITY3);
    Query<? extends PartialEntity> query2 =  GqlQuery.builder(
        Type.FULL, "select * from " + KIND2 + " order by __key__").build();
    QueryResult<? extends PartialEntity> results2 = datastore.run(query2);
    assertTrue(results2.hasNext());
    assertEquals(ENTITY2, results2.next());
    assertTrue(results2.hasNext());
    assertEquals(ENTITY3, results2.next());
    assertFalse(results2.hasNext());

    query1 = GqlQuery.builder(Type.FULL, "select * from bla").build();
    results1 = datastore.run(query1);
    assertFalse(results1.hasNext());

    Query<Key> keyOnlyQuery =
        GqlQuery.builder(Type.KEY_ONLY, "select __key__ from " + KIND1).build();
    QueryResult<Key> keyOnlyResults = datastore.run(keyOnlyQuery);
    assertTrue(keyOnlyResults.hasNext());
    assertEquals(KEY1, keyOnlyResults.next());
    assertFalse(keyOnlyResults.hasNext());

    GqlQuery<ProjectionEntity> keyProjectionQuery = GqlQuery.builder(
        Type.PROJECTION, "select __key__ from " + KIND1).build();
    QueryResult<ProjectionEntity> keyProjectionResult = datastore.run(keyProjectionQuery);
    assertTrue(keyProjectionResult.hasNext());
    ProjectionEntity projectionEntity = keyProjectionResult.next();
    assertEquals(KEY1, projectionEntity.key());
    assertTrue(projectionEntity.properties().isEmpty());
    assertFalse(keyProjectionResult.hasNext());

    GqlQuery<ProjectionEntity> projectionQuery = GqlQuery.builder(
        Type.PROJECTION, "select str, date from " + KIND1).build();

    QueryResult<ProjectionEntity> projectionResult = datastore.run(projectionQuery);
    assertTrue(projectionResult.hasNext());
    projectionEntity = projectionResult.next();
    assertEquals("str", projectionEntity.getString("str"));
    assertEquals(DATE_TIME_VALUE.get(), projectionEntity.getDateTime("date"));
    assertEquals(DATE_TIME_VALUE.get().timestampMicroseconds(),
        projectionEntity.getLong("date"));
    assertEquals(2, projectionEntity.names().size());
    assertFalse(projectionResult.hasNext());
  }

  @Test
  public void testRunGqlQueryWithCasting() {
    @SuppressWarnings("unchecked")
    Query<Entity> query1 = (Query<Entity>) GqlQuery.builder("select * from " + KIND1).build();
    QueryResult<Entity> results1 = datastore.run(query1);
    assertTrue(results1.hasNext());
    assertEquals(ENTITY1, results1.next());
    assertFalse(results1.hasNext());

    Query<?> query2 = GqlQuery.builder("select * from " + KIND1).build();
    QueryResult<?> results2 = datastore.run(query2);
    assertSame(Entity.class, results2.resultClass());
    @SuppressWarnings("unchecked")
    QueryResult<Entity> results3 = (QueryResult<Entity>) results2;
    assertTrue(results3.hasNext());
    assertEquals(ENTITY1, results3.next());
    assertFalse(results3.hasNext());
  }

  @Test
  public void testRunStructuredQuery() throws DatastoreException {
    StructuredQuery<Entity> query =
        StructuredQuery.builder().kind(KIND1).orderBy(OrderBy.asc("__key__")).build();
    QueryResult<Entity> results1 = datastore.run(query);
    assertTrue(results1.hasNext());
    assertEquals(ENTITY1, results1.next());
    assertFalse(results1.hasNext());

    StructuredQuery<Key> keyOnlyQuery =  StructuredQuery.keyOnlyBuilder().kind(KIND1).build();
    QueryResult<Key> results2 = datastore.run(keyOnlyQuery);
    assertTrue(results2.hasNext());
    assertEquals(ENTITY1.key(), results2.next());
    assertFalse(results2.hasNext());

    StructuredQuery<ProjectionEntity> keyOnlyProjectionQuery = StructuredQuery.projectionBuilder()
        .kind(KIND1).projection(Projection.property("__key__")).build();
    QueryResult<ProjectionEntity> results3 = datastore.run(keyOnlyProjectionQuery);
    assertTrue(results3.hasNext());
    ProjectionEntity projectionEntity = results3.next();
    assertEquals(ENTITY1.key(), projectionEntity.key());
    assertTrue(projectionEntity.names().isEmpty());
    assertFalse(results2.hasNext());

    StructuredQuery<ProjectionEntity> projectionQuery = StructuredQuery.projectionBuilder()
        .kind(KIND2)
        .projection(Projection.property("age"), Projection.first("name"))
        .filter(PropertyFilter.gt("age", 18))
        .groupBy("age")
        .orderBy(OrderBy.asc("age"))
        .limit(10)
        .build();

    QueryResult<ProjectionEntity> results4 = datastore.run(projectionQuery);
    assertTrue(results4.hasNext());
    ProjectionEntity entity = results4.next();
    assertEquals(ENTITY2.key(), entity.key());
    assertEquals(20, entity.getLong("age"));
    assertEquals("Dan", entity.getString("name"));
    assertEquals(2, entity.properties().size());
    assertFalse(results4.hasNext());
    // TODO(ozarov): construct a test to verify nextQuery/pagination
  }

  @Test
  public void testAllocateId() {
    KeyFactory keyFactory = helper.newKeyFactory().kind(KIND1);
    PartialKey pk1 = keyFactory.newKey();
    Key key1 = keyFactory.allocateId();
    assertEquals(key1.dataset(), pk1.dataset());
    assertEquals(key1.namespace(), pk1.namespace());
    assertEquals(key1.ancestors(), pk1.ancestors());
    assertEquals(key1.kind(), pk1.kind());
    assertTrue(key1.hasId());
    assertFalse(key1.hasName());
    assertEquals(Key.builder(pk1, key1.id()).build(), key1);

    Key key2 = datastore.allocateId(pk1);
    assertNotEquals(key1, key2);
    assertEquals(Key.builder(pk1, key2.id()).build(), key2);

    Key key3 = datastore.allocateId(key1);
    assertNotEquals(key1, key3);
    assertEquals(Key.builder(pk1, key3.id()).build(), key3);
  }

  @Test
  public void testAllocateIdArray() {
    KeyFactory keyFactory = helper.newKeyFactory().kind(KIND1);
    PartialKey partialKey1 = keyFactory.newKey();
    PartialKey partialKey2 = keyFactory.kind(KIND2).ancestors(PathElement.of(KIND1, 10)).newKey();
    Key key3 = keyFactory.newKey("name");
    Key key4 = keyFactory.newKey(1);
    List<Key> result =
        datastore.allocateId(partialKey1, partialKey2, key3, key4, partialKey1, key3);
    assertEquals(6, result.size());
    assertEquals(Key.builder(partialKey1, result.get(0).id()).build(), result.get(0));
    assertEquals(Key.builder(partialKey1, result.get(4).id()).build(), result.get(4));
    assertEquals(Key.builder(partialKey2, result.get(1).id()).build(), result.get(1));
    assertEquals(Key.builder(key3).id(result.get(2).id()).build(), result.get(2));
    assertEquals(Key.builder(key3).id(result.get(5).id()).build(), result.get(5));
    assertEquals(Key.builder(key4).id(result.get(3).id()).build(), result.get(3));
  }

  @Test
  public void testGet() {
    Entity entity = datastore.get(KEY3);
    assertNull(entity);

    entity = datastore.get(KEY1);
    assertEquals(ENTITY1, entity);
    StringValue value1 = entity.getValue("str");
    assertEquals(STR_VALUE, value1);
    BooleanValue value2 = entity.getValue("bool");
    assertEquals(BOOL_VALUE, value2);
    ListValue value3 = entity.getValue("list");
    assertEquals(LIST_VALUE2, value3);
    DateTimeValue value4 = entity.getValue("date");
    assertEquals(DATE_TIME_VALUE, value4);
    PartialEntity value5 = entity.getEntity("partial1");
    assertEquals(PARTIAL_ENTITY1, value5);
    assertEquals(5, entity.names().size());
    assertFalse(entity.contains("bla"));
  }

  @Test
  public void testGetArray() {
    datastore.put(ENTITY3);
    Iterator<Entity> result =
        helper.fetch(KEY1, Key.builder(KEY1).name("bla").build(), KEY2, KEY3).iterator();
    assertEquals(ENTITY1, result.next());
    assertNull(result.next());
    assertEquals(ENTITY2, result.next());
    Entity entity3 = result.next();
    assertEquals(ENTITY3, entity3);
    assertTrue(entity3.isNull("null"));
    assertFalse(entity3.getBoolean("bool"));
    assertEquals(LIST_VALUE2.get(), entity3.getList("list"));
    PartialEntity partial1 = entity3.getEntity("partial1");
    Entity partial2 = entity3.getEntity("partial2");
    assertEquals(PARTIAL_ENTITY2, partial1);
    assertEquals(ENTITY2, partial2);
    assertEquals(Value.Type.BOOLEAN, entity3.getValue("bool").type());
    assertEquals(6, entity3.names().size());
    assertFalse(entity3.contains("bla"));
    try {
      entity3.getString("str");
      fail("Expecting a failure");
    } catch (DatastoreServiceException expected) {
      // expected - no such property
    }
    assertFalse(result.hasNext());
    // TODO(ozarov): construct a test to verify more results
  }

  @Test
  public void testAddEntity() {
    List<Entity> keys = helper.fetch(ENTITY1.key(), ENTITY3.key());
    assertEquals(ENTITY1, keys.get(0));
    assertNull(keys.get(1));
    assertEquals(2, keys.size());

    try {
      datastore.add(ENTITY1);
      fail("Expecting a failure");
    } catch (DatastoreServiceException expected) {
      // expected;
    }
    datastore.add(ENTITY3);
    assertEquals(ENTITY3, datastore.get(ENTITY3.key()));
  }

  @Test
  public void testAddPartialEntity() {
    List<Entity> keys = helper.fetch(ENTITY1.key(), ENTITY3.key());
    assertEquals(ENTITY1, keys.get(0));
    assertNull(keys.get(1));
    assertEquals(2, keys.size());

    try {
      datastore.add(ENTITY1);
      fail("Expecting a failure");
    } catch (DatastoreServiceException expected) {
      // expected;
    }

    PartialEntity pe = PartialEntity.builder(PARTIAL_ENTITY2).key(KEY5).build();
    List<Entity> response = datastore.add(PARTIAL_ENTITY3, ENTITY3, PARTIAL_ENTITY3, pe);
    assertEquals(4, response.size());
    assertEquals(PARTIAL_ENTITY3.properties(), response.get(0).properties());
    assertEquals(PARTIAL_ENTITY3.properties(), datastore.get(response.get(0).key()).properties());
    assertSame(ENTITY3, response.get(1));
    assertEquals(ENTITY3, datastore.get(response.get(1).key()));
    assertEquals(PARTIAL_ENTITY3.properties(), response.get(2).properties());
    assertEquals(PARTIAL_ENTITY3.properties(), datastore.get(response.get(2).key()).properties());
    assertEquals(pe.properties(), response.get(3).properties());
    assertEquals(pe.key(), response.get(3).key());
    assertEquals(pe.properties(), datastore.get(response.get(3).key()).properties());
    assertEquals(pe.key(), datastore.get(response.get(3).key()).key());
    assertEquals(pe, response.get(3));
    assertEquals(datastore.get(response.get(3).key()), response.get(3));
  }

  @Test
  public void testUpdate() {
    List<Entity> keys = helper.fetch(ENTITY1.key(), ENTITY3.key());
    assertEquals(ENTITY1, keys.get(0));
    assertNull(keys.get(1));
    assertEquals(2, keys.size());

    try {
      datastore.update(ENTITY3);
      fail("Expecting a failure");
    } catch (DatastoreServiceException expected) {
      // expected;
    }
    datastore.add(ENTITY3);
    assertEquals(ENTITY3, datastore.get(ENTITY3.key()));
    Entity entity3 = Entity.builder(ENTITY3).clear().set("bla", new NullValue()).build();
    assertNotEquals(ENTITY3, entity3);
    datastore.update(entity3);
    assertEquals(entity3, datastore.get(ENTITY3.key()));
  }

  @Test
  public void testPut() {
    Iterator<Entity> keys = helper.fetch(ENTITY1.key(), ENTITY2.key(), ENTITY3.key()).iterator();
    assertEquals(ENTITY1, keys.next());
    assertEquals(ENTITY2, keys.next());
    assertNull(keys.next());
    assertFalse(keys.hasNext());

    Entity entity2 = Entity.builder(ENTITY2).clear().set("bla", new NullValue()).build();
    assertNotEquals(ENTITY2, entity2);
    datastore.put(ENTITY3, ENTITY1, entity2);
    keys = helper.fetch(ENTITY1.key(), ENTITY2.key(), ENTITY3.key()).iterator();
    assertEquals(ENTITY1, keys.next());
    assertEquals(entity2, keys.next());
    assertEquals(ENTITY3, keys.next());
    assertFalse(keys.hasNext());
  }

  @Test
  public void testDelete() {
    Iterator<Entity> keys = helper.fetch(ENTITY1.key(), ENTITY2.key(), ENTITY3.key()).iterator();
    assertEquals(ENTITY1, keys.next());
    assertEquals(ENTITY2, keys.next());
    assertNull(keys.next());
    assertFalse(keys.hasNext());
    datastore.delete(ENTITY1.key(), ENTITY2.key(), ENTITY3.key());
    keys = helper.fetch(ENTITY1.key(), ENTITY2.key(), ENTITY3.key()).iterator();
    assertNull(keys.next());
    assertNull(keys.next());
    assertNull(keys.next());
    assertFalse(keys.hasNext());
  }

  @Test
  public void testKeyFactory() {
    KeyFactory keyFactory = new KeyFactory(datastore).kind(KIND1);
    assertEquals(PARTIAL_KEY1, keyFactory.newKey());
    assertEquals(PartialKey.builder(PARTIAL_KEY1).kind(KIND2).build(),
        new KeyFactory(datastore).kind(KIND2).newKey());
    assertEquals(KEY1, keyFactory.newKey("name"));
    assertEquals(Key.builder(KEY1).id(2).build(), keyFactory.newKey(2));
  }
}
