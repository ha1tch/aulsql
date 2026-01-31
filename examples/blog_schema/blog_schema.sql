-- Blog Application Schema (T-SQL)
-- For use with aul/iaul

-- Users table
CREATE TABLE users (
    id INT IDENTITY(1,1) PRIMARY KEY,
    username NVARCHAR(50) NOT NULL UNIQUE,
    email NVARCHAR(100) NOT NULL UNIQUE,
    password_hash NVARCHAR(255) NOT NULL,
    display_name NVARCHAR(100),
    bio NVARCHAR(MAX),
    avatar_url NVARCHAR(500),
    is_admin BIT DEFAULT 0,
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
)
GO

-- Categories table
CREATE TABLE categories (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(50) NOT NULL UNIQUE,
    slug NVARCHAR(50) NOT NULL UNIQUE,
    description NVARCHAR(255),
    parent_id INT NULL,
    sort_order INT DEFAULT 0,
    FOREIGN KEY (parent_id) REFERENCES categories(id)
)
GO

-- Posts table
CREATE TABLE posts (
    id INT IDENTITY(1,1) PRIMARY KEY,
    author_id INT NOT NULL,
    title NVARCHAR(200) NOT NULL,
    slug NVARCHAR(200) NOT NULL UNIQUE,
    excerpt NVARCHAR(500),
    content NVARCHAR(MAX) NOT NULL,
    status NVARCHAR(20) DEFAULT 'draft',
    featured_image NVARCHAR(500),
    view_count INT DEFAULT 0,
    published_at DATETIME,
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (author_id) REFERENCES users(id)
)
GO

-- Post-Category junction table
CREATE TABLE post_categories (
    post_id INT NOT NULL,
    category_id INT NOT NULL,
    PRIMARY KEY (post_id, category_id),
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
)
GO

-- Tags table
CREATE TABLE tags (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(50) NOT NULL UNIQUE,
    slug NVARCHAR(50) NOT NULL UNIQUE
)
GO

-- Post-Tag junction table
CREATE TABLE post_tags (
    post_id INT NOT NULL,
    tag_id INT NOT NULL,
    PRIMARY KEY (post_id, tag_id),
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
)
GO

-- Comments table
CREATE TABLE comments (
    id INT IDENTITY(1,1) PRIMARY KEY,
    post_id INT NOT NULL,
    author_id INT NULL,
    parent_id INT NULL,
    author_name NVARCHAR(100),
    author_email NVARCHAR(100),
    content NVARCHAR(MAX) NOT NULL,
    is_approved BIT DEFAULT 0,
    created_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
    FOREIGN KEY (author_id) REFERENCES users(id),
    FOREIGN KEY (parent_id) REFERENCES comments(id)
)
GO

-- Indexes
CREATE INDEX idx_posts_author ON posts(author_id)
GO
CREATE INDEX idx_posts_status ON posts(status)
GO
CREATE INDEX idx_posts_published ON posts(published_at)
GO
CREATE INDEX idx_comments_post ON comments(post_id)
GO
CREATE INDEX idx_comments_parent ON comments(parent_id)
GO

-- Sample data: Users
INSERT INTO users (username, email, password_hash, display_name, is_admin)
VALUES ('admin', 'admin@blog.local', 'hashed_pw_1', 'Administrator', 1)
GO
INSERT INTO users (username, email, password_hash, display_name, is_admin)
VALUES ('alice', 'alice@example.com', 'hashed_pw_2', 'Alice Writer', 0)
GO
INSERT INTO users (username, email, password_hash, display_name, is_admin)
VALUES ('bob', 'bob@example.com', 'hashed_pw_3', 'Bob Blogger', 0)
GO

-- Sample data: Categories
INSERT INTO categories (name, slug, description, sort_order)
VALUES ('Technology', 'technology', 'Tech news and tutorials', 1)
GO
INSERT INTO categories (name, slug, description, sort_order)
VALUES ('Lifestyle', 'lifestyle', 'Life tips and stories', 2)
GO
INSERT INTO categories (name, slug, description, sort_order)
VALUES ('Travel', 'travel', 'Adventures around the world', 3)
GO

-- Sample data: Tags
INSERT INTO tags (name, slug) VALUES ('programming', 'programming')
GO
INSERT INTO tags (name, slug) VALUES ('sql', 'sql')
GO
INSERT INTO tags (name, slug) VALUES ('databases', 'databases')
GO
INSERT INTO tags (name, slug) VALUES ('tips', 'tips')
GO
INSERT INTO tags (name, slug) VALUES ('tutorial', 'tutorial')
GO

-- Sample data: Posts
INSERT INTO posts (author_id, title, slug, excerpt, content, status, published_at)
VALUES (1, 'Getting Started with SQL', 'getting-started-sql',
        'Learn the basics of SQL queries',
        'SQL is the standard language for working with relational databases. In this post, we will cover SELECT, INSERT, UPDATE, and DELETE statements.',
        'published', GETDATE())
GO
INSERT INTO posts (author_id, title, slug, excerpt, content, status, published_at)
VALUES (2, 'Advanced Query Techniques', 'advanced-query-techniques',
        'Take your SQL skills to the next level',
        'Once you master the basics, you can explore JOINs, subqueries, CTEs, and window functions.',
        'published', GETDATE())
GO
INSERT INTO posts (author_id, title, slug, excerpt, content, status, published_at)
VALUES (3, 'My Database Journey', 'my-database-journey',
        'How I learned to love databases',
        'It all started when I needed to store data for my first web application...',
        'draft', NULL)
GO

-- Sample data: Post-Category relationships
INSERT INTO post_categories (post_id, category_id) VALUES (1, 1)
GO
INSERT INTO post_categories (post_id, category_id) VALUES (2, 1)
GO
INSERT INTO post_categories (post_id, category_id) VALUES (3, 1)
GO

-- Sample data: Post-Tag relationships
INSERT INTO post_tags (post_id, tag_id) VALUES (1, 1)
GO
INSERT INTO post_tags (post_id, tag_id) VALUES (1, 2)
GO
INSERT INTO post_tags (post_id, tag_id) VALUES (1, 5)
GO
INSERT INTO post_tags (post_id, tag_id) VALUES (2, 1)
GO
INSERT INTO post_tags (post_id, tag_id) VALUES (2, 2)
GO
INSERT INTO post_tags (post_id, tag_id) VALUES (2, 3)
GO
INSERT INTO post_tags (post_id, tag_id) VALUES (3, 2)
GO
INSERT INTO post_tags (post_id, tag_id) VALUES (3, 3)
GO

-- Sample data: Comments
INSERT INTO comments (post_id, author_id, content, is_approved)
VALUES (1, 3, 'Great introduction! Very helpful.', 1)
GO
INSERT INTO comments (post_id, author_id, author_name, content, is_approved)
VALUES (1, NULL, 'Guest', 'Thanks for sharing!', 0)
GO
INSERT INTO comments (post_id, author_id, content, is_approved)
VALUES (2, 2, 'I use CTEs all the time now.', 1)
GO

-- Verify counts
SELECT 'users' AS table_name, COUNT(*) AS row_count FROM users
UNION ALL
SELECT 'categories', COUNT(*) FROM categories
UNION ALL
SELECT 'posts', COUNT(*) FROM posts
UNION ALL
SELECT 'tags', COUNT(*) FROM tags
UNION ALL
SELECT 'comments', COUNT(*) FROM comments
GO
