/* this user is only defined during the flyway+jooq code generation step */
CREATE USER IF NOT EXISTS 'vmtplatform'@'%' IDENTIFIED BY 'vmturbo';
GRANT ALL PRIVILEGES ON * . * TO 'vmtplatform'@'%';
CREATE USER IF NOT EXISTS 'vmtplatform'@'localhost' IDENTIFIED BY 'vmturbo';
GRANT ALL PRIVILEGES ON * . * TO 'vmtplatform'@'localhost';
FLUSH PRIVILEGES;