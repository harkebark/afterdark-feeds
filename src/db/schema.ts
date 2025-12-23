export type Post = {
  uri: string
  cid: string
  author: string
  text: string

  replyParent: string | null
  replyRoot: string | null

  indexedAt: number

  // arrays
  algoTags: string[]
  tags: string[] | null
  labels: string[] | null

  // json
  embed: any | null

  // optional fields referenced by your sort
  createdAt: number | null
  earliestCreatedIndexedAt: number | null

  // referenced in getPostBySortWeight
  sort_weight: number | null
}

export type SubState = {
  service: string
  cursor: number
}

export type ListMember = {
  did: string
}

// Kysely DB shape (tables)
export type DatabaseSchema = {
  post: {
    uri: string
    cid: string
    author: string
    text: string
    reply_parent: string | null
    reply_root: string | null
    indexed_at: number
    algo_tags: string[]
    tags: string[] | null
    labels: string[] | null
    embed: any | null
    created_at: number | null
    earliest_created_indexed_at: number | null
    sort_weight: number | null
  }

  sub_state: {
    service: string
    cursor: number
  }

  list_members: {
    did: string
  }
}
