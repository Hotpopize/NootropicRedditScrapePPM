# Data Schema Contract (v1.0)

This document formalizes the input data schema for downstream analysis. The NootropicRedditScrapePPM tool cleanly separates data collection from data analysis. Downstream analysis modules (like `llm_coder.py`), along with underlying helpers like `save_collected_data()`, act upon this schema directly. 

Any researcher who prepares data matching this structured dictionary schema can load their data into the pipeline and take advantage of the topic modeling and automated codebook analysis, **regardless of how the data was obtained**.

## PII Constraints (Author Pseudonymization)

To comply with ethical analysis guidelines and privacy terms:
1. **No Personally Identifiable Information (PII) is allowed in this pipeline.**
2. The `author` field must be explicitly substituted prior to being loaded into the application.
3. The schema enforcing this constraint strictly requires the `author` field to be:
   - Evaluated to `[deleted]` or `[removed]`
   - A deterministic hash (e.g., 64-character SHA256 hex string)
   - Replaced by an anonymous placeholder prefix mapping (e.g., `anon_user1`)

## The `CollectedItem` Dictionary Structure

Every data item, whether it corresponds to a post, comment, message, or transcript snippet, must conform to the following schema. In Python, these fields are defined in `core.schemas.CollectedItem`.

### Core Fields

- `id` *(string)*: A unique identifier for the item (e.g. `abc123`).
- `type` *(string)*: Describes the type of data representing hierarchy. Standard values are `'submission'` or `'comment'`. For external data, this could be mapped logically.
- `subreddit` *(string)*: The group or source origin namespace.
- `title` *(string)*: The title of the post or context string.
- `text` *(string)*: The primary body of the content. **This must always be keyed as `'text'`** (even for comments where APIs often use `'body'`).
- `author` *(string)*: The author identifier, subject to the **PII Constraints** above.
- `score` *(integer)*: A quantifiable metric like upvotes or reactions, defaults to `0`.
- `created_utc` *(float)*: Unix timestamp representing when the item was originally created.
- `permalink` *(string)*: A full URL path where the item can be referenced.
- `collected_at` *(string)*: ISO-8601 formatted datetime string marking when the data hit your local ingestion buffer.
- `data_source` *(string)*: A label for where this data came from. (Default: `'praw'`).

### Optional Fields

- `num_comments` *(integer, optional)*: Total count of downstream replies underneath this specific item.
- `url` *(string, optional)*: If the item represents an external link, place the URL here.
- `post_id` *(string, optional)*: If `type` is a `'comment'` or reply, this holds the ID of its parent `submission` to allow relational tracking.

### `metadata` Dictionary Fields

Each item must include a `metadata` dictionary adhering to `core.schemas.ItemMetadata`. This section accommodates tool-specific states dynamically.

All fields are optional or have defaults:
- `nsfw` *(boolean)*: True if the content is marked not safe for work. Default `False`.
- `content_status` *(string)*: State of the content text. E.g., `'available'`, `'removed'`, `'author_deleted'`, `'empty'`.
- `content_type` *(string)*: Media subtype classification. `'text'`, `'image'`, `'video'`, `'link'`.
- `language_flag` *(string)*: `'english'`, `'likely_non_english'`, `'not_checked'`.
- `text_length` *(integer)*: Character length of the `text` field.
- `word_count` *(integer)*: Absolute word count of the `text` field.
- `was_truncated` *(boolean)*: If the text hit an ingestion length cap.
- `auto_tags` *(list of strings)*: Initial keyword flags aligned to codebooks.
- `collection_hash` *(string)*: Unique hash ID linking back to the `replicability_log`.

## Example JSON payload

```json
{
  "id": "1ex9a1a",
  "type": "submission",
  "subreddit": "Nootropics",
  "title": "Experience with Bacopa Monnieri",
  "text": "I've been taking this for a week and noticing distinct calmness but slight lethargy.",
  "author": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "score": 12,
  "created_utc": 1698249600.0,
  "permalink": "https://reddit.com/r/Nootropics/comments/1ex9a1a/experience",
  "collected_at": "2023-10-31T08:00:00.000Z",
  "data_source": "custom_importer",
  "metadata": {
    "nsfw": false,
    "content_status": "available",
    "content_type": "text",
    "language_flag": "english",
    "text_length": 83,
    "word_count": 13,
    "was_truncated": false,
    "collection_hash": "a1b2c3d4e5f67890"
  }
}
```
