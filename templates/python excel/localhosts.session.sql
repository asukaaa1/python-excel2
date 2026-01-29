-- See all users
SELECT * FROM users;

-- See all passwords
SELECT * FROM password_entries;

-- See passwords with user info
SELECT 
    p.title,
    p.username,
    p.url,
    p.category,
    u.username as owner
FROM password_entries p
JOIN users u ON p.user_id = u.id;