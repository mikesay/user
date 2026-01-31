FROM mongo:8.2.4

USER root
ADD ./scripts /tmp/scripts
RUN chmod +x /tmp/scripts/mongo_create_insert.sh

# 1. Create custom dbpath
RUN mkdir -p /data/db-users && chown -R mongodb:mongodb /data/db-users

# 2. Start Mongo, run script, and shutdown properly
# Note: Modern Mongo uses YAML config, but we pass --dbpath directly for the build step
RUN su - mongodb -c "mongod --fork --logpath /tmp/mongodb.log --dbpath /data/db-users" \
    && /tmp/scripts/mongo_create_insert.sh \
    && su - mongodb -c "mongod --dbpath /data/db-users --shutdown"

USER mongodb
VOLUME /data/db-users

# 3. Use --dbpath in CMD (no-auth as requested)
CMD ["mongod", "--dbpath", "/data/db-users", "--bind_ip_all"]
