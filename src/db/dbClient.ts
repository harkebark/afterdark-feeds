import dotenv from 'dotenv'
dotenv.config()

import { Pool } from 'pg'
import { Kysely, PostgresDialect, sql } from 'kysely'
import type { DatabaseSchema } from './schema'
import { InvalidRequestError } from '@atproto/xrpc-server'

function parsePgBool(v: string | undefined): boolean {
  return v === '1' || v?.toLowerCase() === 'true'
}

function mapPostRowToApi(row: any) {
  // convert snake_case DB row to your existing camelCase expectations
  return {
    uri: row.uri,
    cid: row.cid,
    author: row.author,
    text: row.text,
    replyParent: row.reply_parent,
    replyRoot: row.reply_root,
    indexedAt: row.indexed_at,
    algoTags: row.algo_tags ?? [],
    tags: row.tags ?? null,
    labels: row.labels ?? null,
    embed: row.embed ?? null,
    createdAt: row.created_at ?? null,
    earliestCreatedIndexedAt: row.earliest_created_indexed_at ?? null,
    sort_weight: row.sort_weight ?? null,
  }
}

class dbSingleton {
  private static instance: dbSingleton | null = null

  pool: Pool
  db: Kysely<DatabaseSchema>

  private constructor() {
    const connectionString = process.env.FEEDGEN_POSTGRES_CONNECTION_STRING
    if (!connectionString) {
      throw new Error('Missing FEEDGEN_POSTGRES_CONNECTION_STRING')
    }

    const useSSL =
      parsePgBool(process.env.FEEDGEN_POSTGRES_SSL) ||
      // common for managed providers
      connectionString.includes('sslmode=require')

    this.pool = new Pool({
      connectionString,
      ssl: useSSL ? { rejectUnauthorized: false } : undefined,
      max: Number(process.env.FEEDGEN_POSTGRES_POOL_MAX ?? '20'),
    })

    this.db = new Kysely<DatabaseSchema>({
      dialect: new PostgresDialect({ pool: this.pool }),
    })
  }

  static getInstance(): dbSingleton {
    if (dbSingleton.instance === null) {
      dbSingleton.instance = new dbSingleton()
    }
    return dbSingleton.instance
  }

  /**
   * Call this once at process start (optional).
   * If you prefer explicit migrations, you can skip auto-init.
   */
  async init() {
    await this.ensureSchema()
  }

  async ensureSchema() {
    // Minimal schema + indexes to match your query patterns.
    // This is safe to run repeatedly (CREATE IF NOT EXISTS).
    const client = await this.pool.connect()
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS post (
          uri TEXT PRIMARY KEY,
          cid TEXT NOT NULL,
          author TEXT NOT NULL,
          text TEXT NOT NULL,

          reply_parent TEXT NULL,
          reply_root TEXT NULL,

          indexed_at BIGINT NOT NULL,

          algo_tags TEXT[] NOT NULL DEFAULT '{}',
          tags TEXT[] NULL,
          labels TEXT[] NULL,

          embed JSONB NULL,

          created_at BIGINT NULL,
          earliest_created_indexed_at BIGINT NULL,

          sort_weight DOUBLE PRECISION NULL
        );

        CREATE TABLE IF NOT EXISTS sub_state (
          service TEXT PRIMARY KEY,
          cursor BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS list_members (
          did TEXT PRIMARY KEY
        );

        -- Helpful indexes
        CREATE INDEX IF NOT EXISTS idx_post_indexed_at ON post (indexed_at DESC, cid DESC);
        CREATE INDEX IF NOT EXISTS idx_post_author ON post (author);
        CREATE INDEX IF NOT EXISTS idx_post_reply_root ON post (reply_root);
        CREATE INDEX IF NOT EXISTS idx_post_algo_tags_gin ON post USING GIN (algo_tags);
        CREATE INDEX IF NOT EXISTS idx_post_labels_gin ON post USING GIN (labels);
        CREATE INDEX IF NOT EXISTS idx_post_sort_weight ON post (sort_weight DESC NULLS LAST);
      `)
    } finally {
      client.release()
    }
  }

  // --- Utility deletes -------------------------------------------------------

  async deleteManyURI(collection: string, uris: string[]) {
    if (collection !== 'post') return
    if (uris.length === 0) return
    await this.db.deleteFrom('post').where('uri', 'in', uris).execute()
  }

  async deleteManyDID(collection: string, dids: string[]) {
    if (collection !== 'list_members') return
    if (dids.length === 0) return
    await this.db.deleteFrom('list_members').where('did', 'in', dids).execute()
  }

  // --- Upserts ---------------------------------------------------------------

  async replaceOneURI(_collection: string, uri: string, data: any) {
    // Mongo used insert then replace; here it's a single UPSERT.
    // Expect data fields in your existing shape.
    const algoTags: string[] = Array.isArray(data.algoTags) ? data.algoTags : []
    const tags: string[] | null = Array.isArray(data.tags) ? data.tags : null
    const labels: string[] | null = Array.isArray(data.labels) ? data.labels : null

    await this.db
      .insertInto('post')
      .values({
        uri,
        cid: data.cid,
        author: data.author,
        text: data.text ?? '',
        reply_parent: data.replyParent ?? null,
        reply_root: data.replyRoot ?? null,
        indexed_at: Number(data.indexedAt ?? Date.now()),
        algo_tags: algoTags,
        tags,
        labels,
        embed: data.embed ?? null,
        created_at: data.createdAt ?? null,
        earliest_created_indexed_at: data.earliestCreatedIndexedAt ?? null,
        sort_weight: data.sort_weight ?? null,
      })
      .onConflict((oc) =>
        oc.column('uri').doUpdateSet({
          cid: (eb) => eb.ref('excluded.cid'),
          author: (eb) => eb.ref('excluded.author'),
          text: (eb) => eb.ref('excluded.text'),
          reply_parent: (eb) => eb.ref('excluded.reply_parent'),
          reply_root: (eb) => eb.ref('excluded.reply_root'),
          indexed_at: (eb) => eb.ref('excluded.indexed_at'),
          algo_tags: (eb) => eb.ref('excluded.algo_tags'),
          tags: (eb) => eb.ref('excluded.tags'),
          labels: (eb) => eb.ref('excluded.labels'),
          embed: (eb) => eb.ref('excluded.embed'),
          created_at: (eb) => eb.ref('excluded.created_at'),
          earliest_created_indexed_at: (eb) =>
            eb.ref('excluded.earliest_created_indexed_at'),
          sort_weight: (eb) => eb.ref('excluded.sort_weight'),
        }),
      )
      .execute()
  }

  async replaceOneDID(collection: string, did: string, data: any) {
    if (collection !== 'list_members') return

    await this.db
      .insertInto('list_members')
      .values({ did: data.did ?? did })
      .onConflict((oc) => oc.column('did').doNothing())
      .execute()
  }

  // --- Existing methods rewritten -------------------------------------------

  async getPostBySortWeight(
    _collection: string,
    limit = 50,
    cursor: string | undefined = undefined,
  ) {
    // Mongo skip-based pagination; keep behavior for compatibility.
    let start = 0
    if (cursor !== undefined) start = Number.parseInt(cursor, 10) || 0

    const rows = await this.db
      .selectFrom('post')
      .selectAll()
      .orderBy('sort_weight', 'desc')
      .offset(start)
      .limit(limit)
      .execute()

    return rows.map(mapPostRowToApi)
  }

  async aggregatePostsByRepliesToCollection(
    _collection: string,
    tag: string,
    threshold: number,
    out: string,
    limit: number = 10000,
  ) {
    // Mongo used $merge into an output collection.
    // In Postgres, we’ll materialize into a table named by `out`:
    //   out(_id TEXT PRIMARY KEY, count BIGINT, indexed_at BIGINT)
    //
    // NOTE: This uses raw SQL because the table name is dynamic.
    const indexedAt = Date.now()

    const client = await this.pool.connect()
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS "${out}" (
          _id TEXT PRIMARY KEY,
          count BIGINT NOT NULL,
          indexed_at BIGINT NOT NULL
        );
      `)

      await client.query(
        `
        INSERT INTO "${out}" (_id, count, indexed_at)
        SELECT reply_root AS _id, COUNT(*)::bigint AS count, $3::bigint AS indexed_at
        FROM post
        WHERE $1 = ANY(algo_tags) AND reply_root IS NOT NULL
        GROUP BY reply_root
        HAVING COUNT(*) > $2
        ORDER BY count DESC
        LIMIT $4
        ON CONFLICT (_id)
        DO UPDATE SET count = EXCLUDED.count, indexed_at = EXCLUDED.indexed_at;
        `,
        [tag, threshold, indexedAt, limit],
      )

      // delete stale rows (those not part of this run)
      await client.query(
        `DELETE FROM "${out}" WHERE indexed_at <> $1::bigint;`,
        [indexedAt],
      )
    } finally {
      client.release()
    }
  }

  async getCollection(collection: string) {
    if (collection === 'post') {
      const rows = await this.db.selectFrom('post').selectAll().execute()
      return rows.map(mapPostRowToApi)
    }
    if (collection === 'sub_state') {
      return await this.db.selectFrom('sub_state').selectAll().execute()
    }
    if (collection === 'list_members') {
      return await this.db.selectFrom('list_members').selectAll().execute()
    }
    return []
  }

  async insertOrReplaceRecord(
    query: any,
    data: any,
    collection: string,
  ) {
    // This method was Mongo-generic; in Postgres it’s better to call
    // your explicit upsert methods. Keep a limited compatible behavior.
    if (collection === 'post') {
      await this.replaceOneURI('post', data.uri, data)
      return
    }
    if (collection === 'sub_state') {
      await this.updateSubStateCursor(data.service, data.cursor)
      return
    }
    if (collection === 'list_members') {
      await this.replaceOneDID('list_members', data.did, data)
      return
    }
    // ignore unknown collections
  }

  async updateSubStateCursor(service: string, cursor: number) {
    await this.db
      .insertInto('sub_state')
      .values({ service, cursor })
      .onConflict((oc) =>
        oc.column('service').doUpdateSet({ cursor: (eb) => eb.ref('excluded.cursor') }),
      )
      .execute()
  }

  async getSubStateCursor(service: string) {
    const row = await this.db
      .selectFrom('sub_state')
      .selectAll()
      .where('service', '=', service)
      .executeTakeFirst()

    if (!row) return { service, cursor: 0 }
    return row
  }

  async getLatestPostsForTag({
    tag,
    limit = 50,
    cursor = undefined,
    mediaOnly = false,
    nsfwOnly = false,
    excludeNSFW = false,
    sortOrder = 'desc',
  }: {
    tag: string
    limit?: number
    cursor?: string | undefined
    mediaOnly?: boolean
    nsfwOnly?: boolean
    excludeNSFW?: boolean
    sortOrder?: 'asc' | 'desc'
  }) {
    let indexedAtCursor: number | undefined
    let cidCursor: string | undefined

    if (cursor !== undefined) {
      const parts = cursor.split('::')
      if (parts.length !== 2) throw new InvalidRequestError('malformed cursor')
      indexedAtCursor = Number.parseInt(parts[0]!, 10)
      cidCursor = parts[1]!
      if (!Number.isFinite(indexedAtCursor) || !cidCursor) {
        throw new InvalidRequestError('malformed cursor')
      }
    }

    // base query
    let q = this.db
      .selectFrom('post')
      .selectAll()
      .where(sql`${sql.lit(tag)} = ANY(post.algo_tags)`)

    if (indexedAtCursor !== undefined) {
      q = q
        .where('indexed_at', '<=', indexedAtCursor)
        .where('cid', '!=', cidCursor!)
    }

    if (mediaOnly) {
      // JSONB key existence checks
      q = q.where(
        sql`(post.embed ? 'images') OR (post.embed ? 'video') OR (post.embed ? 'media')`,
      )
    }

    const nsfwSet = ['porn', 'nudity', 'sexual', 'underwear']
    if (nsfwOnly) {
      q = q.where(sql`post.labels && ${sql.array(nsfwSet, 'text')}`)
    }
    if (excludeNSFW) {
      q = q.where(sql`NOT (post.labels && ${sql.array(nsfwSet, 'text')})`)
    }

    const rows = await q
      .orderBy('earliest_created_indexed_at', sortOrder)
      .orderBy('created_at', sortOrder)
      .orderBy('indexed_at', sortOrder)
      .orderBy('cid', 'desc')
      .limit(limit)
      .execute()

    return rows.map(mapPostRowToApi)
  }

  async getTaggedPostsBetween(tag: string, start: number, end: number) {
    const larger = start > end ? start : end
    const smaller = start > end ? end : start

    const rows = await this.db
      .selectFrom('post')
      .selectAll()
      .where('indexed_at', '<', larger)
      .where('indexed_at', '>', smaller)
      .where(sql`${sql.lit(tag)} = ANY(post.algo_tags)`)
      .orderBy('indexed_at', 'desc')
      .orderBy('cid', 'desc')
      .execute()

    return rows.map(mapPostRowToApi)
  }

  async getUnlabelledPostsWithMedia(limit = 100, lagTime = 5 * 60 * 1000) {
    const cutoff = Date.now() - lagTime

    const rows = await this.db
      .selectFrom('post')
      .selectAll()
      .where(sql`(post.embed ? 'images') OR (post.embed ? 'video') OR (post.embed ? 'media')`)
      .where('labels', 'is', null)
      .where('indexed_at', '<', cutoff)
      .orderBy('indexed_at', 'desc')
      .orderBy('cid', 'desc')
      .limit(limit)
      .execute()

    return rows.map(mapPostRowToApi)
  }

  async updateLabelsForURIs(postEntries: { uri: string; labels: string[] }[]) {
    if (postEntries.length === 0) return

    // batched updates via raw SQL for speed
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      for (const e of postEntries) {
        await client.query(
          `UPDATE post SET labels = $2::text[] WHERE uri = $1;`,
          [e.uri, e.labels],
        )
      }
      await client.query('COMMIT')
    } catch (e) {
      await client.query('ROLLBACK')
      throw e
    } finally {
      client.release()
    }
  }

  async getRecentAuthorsForTag(tag: string, lastMs: number = 600000) {
    const cutoff = Date.now() - lastMs

    const rows = await this.db
      .selectFrom('post')
      .select(['author'])
      .distinct()
      .where('indexed_at', '>', cutoff)
      .where(sql`${sql.lit(tag)} = ANY(post.algo_tags)`)
      .execute()

    return rows.map((r) => r.author)
  }

  async getDistinctFromCollection(collection: string, field: string) {
    // limited support for what you currently use
    if (collection === 'post' && field === 'author') {
      const rows = await this.db
        .selectFrom('post')
        .select(['author'])
        .distinct()
        .execute()
      return rows.map((r) => r.author)
    }

    // for anything else, use raw sql
    const safeCollection = collection.replace(/[^a-zA-Z0-9_]/g, '')
    const safeField = field.replace(/[^a-zA-Z0-9_]/g, '')
    const res = await this.pool.query(
      `SELECT DISTINCT "${safeField}" FROM "${safeCollection}"`,
    )
    return res.rows.map((r) => r[safeField])
  }

  async removeTagFromPostsForAuthor(tag: string, authors: string[]) {
    if (authors.length === 0) return

    await this.db
      .updateTable('post')
      .set({
        algo_tags: sql`array_remove(post.algo_tags, ${tag})`,
      })
      .where('author', 'in', authors)
      .execute()

    await this.deleteUntaggedPosts()
  }

  async removeTagFromOldPosts(tag: string, indexedAt: number) {
    await this.db
      .updateTable('post')
      .set({
        algo_tags: sql`array_remove(post.algo_tags, ${tag})`,
      })
      .where('indexed_at', '<', indexedAt)
      .execute()

    await this.deleteUntaggedPosts()
  }

  async deleteUntaggedPosts() {
    await this.db
      .deleteFrom('post')
      .where(sql`COALESCE(array_length(post.algo_tags, 1), 0) = 0`)
      .execute()
  }

  async getPostForURI(uri: string) {
    const row = await this.db
      .selectFrom('post')
      .selectAll()
      .where('uri', '=', uri)
      .executeTakeFirst()

    return row ? mapPostRowToApi(row) : null
  }
}

const dbClient = dbSingleton.getInstance()
export default dbClient
