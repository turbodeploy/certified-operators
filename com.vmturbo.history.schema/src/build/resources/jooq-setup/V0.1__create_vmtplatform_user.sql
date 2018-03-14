/* this user is only defined during the flyway+jooq code generation step */
GRANT ALL ON * . * to 'vmtplatform'@'%' identified by 'vmturbo';
GRANT ALL ON * . * to 'vmtplatform'@'localhost' identified by 'vmturbo';
FLUSH PRIVILEGES;