/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastrato.gravitino.storage.relational.mapper;

import com.datastrato.gravitino.storage.relational.po.TagMetadataObjectRelPO;
import com.datastrato.gravitino.storage.relational.po.TagPO;
import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface TagMetadataObjectRelMapper {
  String TAG_METADATA_OBJECT_RELATION_TABLE_NAME = "tag_relation_meta";

  @Select(
      "SELECT tm.tag_id as tagId, tm.tag_name as tagName,"
          + " tm.metalake_id as metalakeId, tm.tag_comment as comment, tm.properties as properties,"
          + " tm.audit_info as auditInfo,"
          + " tm.current_version as currentVersion,"
          + " tm.last_version as lastVersion,"
          + " tm.deleted_at as deletedAt"
          + " FROM "
          + TagMetaMapper.TAG_TABLE_NAME
          + " tm JOIN "
          + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
          + " te ON tm.tag_id = te.tag_id"
          + " WHERE te.metadata_object_id = #{metadataObjectId}"
          + " AND te.metadata_object_type = #{metadataObjectType} AND te.deleted_at = 0"
          + " AND tm.deleted_at = 0")
  List<TagPO> listTagPOsByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType);

  @Select(
      "SELECT tm.tag_id as tagId, tm.tag_name as tagName,"
          + " tm.metalake_id as metalakeId, tm.tag_comment as comment, tm.properties as properties,"
          + " tm.audit_info as auditInfo,"
          + " tm.current_version as currentVersion,"
          + " tm.last_version as lastVersion,"
          + " tm.deleted_at as deletedAt"
          + " FROM "
          + TagMetaMapper.TAG_TABLE_NAME
          + " tm JOIN "
          + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
          + " te ON tm.tag_id = te.tag_id"
          + " WHERE te.metadata_object_id = #{metadataObjectId}"
          + " AND te.metadata_object_type = #{metadataObjectType} AND tm.tag_name = #{tagName}"
          + " AND te.deleted_at = 0 AND tm.deleted_at = 0")
  TagPO getTagPOsByMetadataObjectAndTagName(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("tagName") String tagName);

  @Select(
      "SELECT te.tag_id as tagId, te.metadata_object_id as metadataObjectId,"
          + " te.metadata_object_type as metadataObjectType, te.audit_info as auditInfo,"
          + " te.current_version as currentVersion, te.last_version as lastVersion,"
          + " te.deleted_at as deletedAt"
          + " FROM "
          + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
          + " te JOIN "
          + TagMetaMapper.TAG_TABLE_NAME
          + " tm JOIN "
          + MetalakeMetaMapper.TABLE_NAME
          + " mm ON te.tag_id = tm.tag_id AND tm.metalake_id = mm.metalake_id"
          + " WHERE mm.metalake_name = #{metalakeName} AND tm.tag_name = #{tagName}"
          + " AND te.deleted_at = 0 AND tm.deleted_at = 0 AND mm.deleted_at = 0")
  List<TagMetadataObjectRelPO> listTagMetadataObjectRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName);

  @Insert({
    "<script>",
    "INSERT INTO "
        + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + "(tag_id, metadata_object_id, metadata_object_type, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ",
    "<foreach collection='tagRels' item='item' separator=','>",
    "(#{item.tagId},"
        + " #{item.metadataObjectId},"
        + " #{item.metadataObjectType},"
        + " #{item.auditInfo},"
        + " #{item.currentVersion},"
        + " #{item.lastVersion},"
        + " #{item.deletedAt})",
    "</foreach>",
    "</script>"
  })
  void batchInsertTagMetadataObjectRels(@Param("tagRels") List<TagMetadataObjectRelPO> tagRelPOs);

  @Update({
    "<script>",
    "UPDATE "
        + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE tag_id IN",
    "<foreach item='tagId' collection='tagIds' open='(' separator=',' close=')'>",
    "#{tagId}",
    "</foreach>",
    " And metadata_object_id = #{metadataObjectId}"
        + " AND metadata_object_type = #{metadataObjectType} AND deleted_at = 0",
    "</script>"
  })
  void batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("tagIds") List<Long> tagIds);

  @Update(
      "UPDATE "
          + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
          + " te SET te.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE te.tag_id IN (SELECT tm.tag_id FROM "
          + TagMetaMapper.TAG_TABLE_NAME
          + " tm WHERE tm.metalake_id IN (SELECT mm.metalake_id FROM "
          + MetalakeMetaMapper.TABLE_NAME
          + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
          + " AND tm.deleted_at = 0) AND te.deleted_at = 0")
  Integer softDeleteTagMetadataObjectRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName);

  @Update(
      "UPDATE "
          + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
          + " te SET te.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE EXISTS (SELECT * FROM "
          + TagMetaMapper.TAG_TABLE_NAME
          + " tm WHERE tm.metalake_id = #{metalakeId} AND tm.tag_id = te.tag_id"
          + " AND tm.deleted_at = 0) AND te.deleted_at = 0")
  void softDeleteTagMetadataObjectRelsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Delete(
      "DELETE FROM "
          + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteTagEntityRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
