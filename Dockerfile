FROM zooniverse/ruby:2.1.2

ENV DEBIAN_FRONTEND noninteractive

WORKDIR /app/

ADD Gemfile /app/
ADD Gemfile.lock /app/

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y mongodb-clients && \
    bundle install && \
    mkdir /out

ADD backup_mongodb.rb /app/

ENTRYPOINT [ "ruby", "/app/backup_mongodb.rb" ]
