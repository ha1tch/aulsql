# Blog Schema

A simple blog application database schema for use with aul/iaul.

## Tables

| Table | Description |
|-------|-------------|
| users | Blog authors and administrators |
| categories | Hierarchical post categories |
| posts | Blog articles |
| post_categories | Many-to-many: posts to categories |
| tags | Flat labels for posts |
| post_tags | Many-to-many: posts to tags |
| comments | Threaded comments on posts |

## Entity Relationships

```
users 1──────< posts
               │
               ├──< post_categories >──1 categories
               │                            │
               │                            └──< categories (parent)
               ├──< post_tags >──1 tags
               │
               └──< comments
                      │
                      └──< comments (parent)
```

## Loading the Schema

Start aul:

```bash
aul --tds-port 1433
```

Load via iaul:

```bash
iaul -f blog_schema.sql --host localhost --user sa --password test --database master
```

Or interactively:

```
sql> \i blog_schema.sql
```

## Sample Queries

**List published posts with authors:**

```sql
SELECT p.title, u.display_name as author, p.published_at
FROM posts p
JOIN users u ON p.author_id = u.id
WHERE p.status = 'published'
ORDER BY p.published_at DESC
```

**Posts with their categories:**

```sql
SELECT p.title, c.name AS category
FROM posts p
JOIN post_categories pc ON p.id = pc.post_id
JOIN categories c ON pc.category_id = c.id
```

**Posts with their tags:**

```sql
SELECT p.title, t.name AS tag
FROM posts p
JOIN post_tags pt ON p.id = pt.post_id
JOIN tags t ON pt.tag_id = t.id
```

**Comment counts per post:**

```sql
SELECT p.title, COUNT(c.id) as comment_count
FROM posts p
LEFT JOIN comments c ON p.id = c.post_id AND c.is_approved = 1
GROUP BY p.id, p.title
ORDER BY comment_count DESC
```

**Most used tags:**

```sql
SELECT t.name, COUNT(pt.post_id) as usage_count
FROM tags t
LEFT JOIN post_tags pt ON t.id = pt.tag_id
GROUP BY t.id, t.name
ORDER BY usage_count DESC
```

## Sample Data

The schema includes:

- 3 users (admin, alice, bob)
- 3 categories (Technology, Lifestyle, Travel)
- 5 tags (programming, sql, databases, tips, tutorial)
- 3 posts (2 published, 1 draft)
- 3 comments (2 approved, 1 pending)

## Post Status Values

| Status | Description |
|--------|-------------|
| draft | Work in progress, not visible |
| published | Live and visible |
| archived | Hidden from listings |
