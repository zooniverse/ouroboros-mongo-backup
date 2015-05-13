require 'active_support/core_ext/string/inflections'
require 'aws-sdk'
require 'mongo'
require 'mail'
require 'yaml'

config = YAML.load(File.read('/config.yml'))

mongo_host = nil

output_dir = '/out/'

config['mongo']['hosts'].each do |host|
  secondary_check = <<-SH
    mongo --quiet #{host}/admin --eval "
    db.auth('#{config['mongo']['admin']['user']}',
      '#{config['mongo']['admin']['pass']}');
    rs.status().members.forEach(function(member) {
      if(member.self) {
        print(member.stateStr === 'SECONDARY');
      }
    })
    "
  SH
  if `#{ secondary_check }`.chomp == 'true'
    mongo_host = host
    break
  end
end

unless mongo_host != nil
  puts 'No secondary MongoDB server found. Aborting'
  abort
end

puts "Found secondary MongoDB server: #{mongo_host}"

AWS.config access_key_id: config['aws']['s3']['access_key_id'], secret_access_key: config['aws']['s3']['secret_access_key']
s3 = AWS::S3.new
@bucket = s3.buckets[config['aws']['s3']['bucket']]
@mutex = Mutex.new

Mail.defaults do
  delivery_method(:smtp, {
    enable_starttls_auto: true,
    address: config['aws']['ses']['address'],
    port: config['aws']['ses']['port'],
    domain: config['aws']['ses']['domain'],
    authentication: :plain,
    user_name: config['aws']['ses']['user_name'],
    password: config['aws']['ses']['password']
  })
end

@timestamp = Time.now.strftime '%Y-%m-%d'
Dir.chdir output_dir
Dir.mkdir 'backups'
Dir.mkdir "backups/ouroboros_projects"

connection = Mongo::ReplSetConnection.new config['mongo']['hosts'], { name: config['mongo']['ouroboros']['rs_name'] }
mongo = connection[config['mongo']['ouroboros']['db_name']]
mongo.authenticate config['mongo']['ouroboros']['user'], config['mongo']['ouroboros']['pass']

@projects = { }
mongo['projects'].find({ }, { fields: ['name'] }).to_a.each do |project|
  name = project['name']
  @projects[project['_id'].to_s] = {
    name: name,
    output: "#{ name }_#{ @timestamp }",
    subjects: "#{ name }_subject".tableize,
    groups: "#{ name }_group".tableize,
    classifications: "#{ name }_classification".tableize
  }
end

connection.close
mongo = nil; connection = nil

GC.start

mongodump = "mongodump --host #{mongo_host} --db #{config['mongo']['ouroboros']['db_name']} --username #{config['mongo']['ouroboros']['user']} --password #{config['mongo']['ouroboros']['pass']}"
mongoexport = "mongoexport --host #{mongo_host} --db #{config['mongo']['ouroboros']['db_name']} --username #{config['mongo']['ouroboros']['user']} --password #{config['mongo']['ouroboros']['pass']}"
sandboxmongoexport = "mongoexport --host #{config['mongo']['sandbox']['host']} --db #{config['mongo']['sandbox']['db_name']} --username #{config['mongo']['sandbox']['user']} --password #{config['mongo']['sandbox']['pass']}"

def upload(name, path, file_path, id = nil)
  dump_object = @bucket.objects["databases/#{ @timestamp }/#{ path }"]
  dump_object.write file: file_path
  url = dump_object.url_for(:read, expires: 604800, secure: true).to_s
  file_size = `stat -c %s #{ file_path }`

  if id
    @mutex.synchronize do
      @projects[id][:email_line] = "Backed up #{ name } (#{ '%.3f' % (file_size.to_f / 1048576)}) MB (#{ url })"
    end
  else
    "Backed up #{ name } (#{ '%.3f' % (file_size.to_f / 1048576)}) MB (#{ url })"
  end
end

sanitized_subject_fields = %w(activated_at classification_count coords created_at group group_id location metadata project_id random state updated_at workflow_ids zooniverse_id)
sanitized_classification_fields = %w(annotations created_at project_id subject_ids subjects tutorial updated_at user_id workflow_id)
sanitized_group_fields = %w(categories classification_count created_at metadata name project_id project_name random state stats subjects updated_at zooniverse_id)

puts "* Starting sanitized backups"

config['sanitized_projects'].each_pair do |id, emails|
  project = @projects[id]

  puts "    * Backing up #{project[:name]}"

  sanitized_project_threads = []
  sanitized_output = "sanitized_#{ project[:output] }"

  `mkdir -p project_dumps/#{ sanitized_output }`

  if config['sandbox_projects'].include? id
    export_cmd = sandboxmongoexport
  else
    export_cmd = mongoexport
  end

  sanitized_project_threads << Thread.new do
    `#{ export_cmd } --collection #{ project[:classifications] } --fields #{ sanitized_classification_fields.join(',') } --out project_dumps/#{ sanitized_output }/#{ project[:classifications] }.json`
  end

  sanitized_project_threads << Thread.new do
    `#{ export_cmd } --collection #{ project[:subjects] } --fields #{ sanitized_subject_fields.join(',') } --out project_dumps/#{ sanitized_output }/#{ project[:subjects] }.json`
  end

  sanitized_project_threads << Thread.new do
    `#{ export_cmd } --collection #{ project[:groups] } --fields #{ sanitized_group_fields.join(',') } --out project_dumps/#{ sanitized_output }/#{ project[:groups] }.json`
  end

  sleep 1
  sanitized_project_threads.map &:join

  `cd project_dumps; tar czvf #{ sanitized_output }.tar.gz #{ sanitized_output }`
  `rm -rf project_dumps/#{ sanitized_output }`
  `mv project_dumps/#{ sanitized_output }.tar.gz backups/ouroboros_projects/#{ sanitized_output }.tar.gz`

  path = "ouroboros_projects/#{ sanitized_output }.tar.gz"
  upload project[:name].titleize, path, "backups/#{ path }", id

  mail = Mail.new do
    from 'team@zooniverse.org'
    to emails
    cc 'sysadmins@zooniverse.org'
    subject "Sanitized #{ project[:name] } MongoDB Backup #{ @timestamp }"
    body project[:email_line]
  end
  mail.deliver!
  project.delete :email_line
end

puts "* Starting per-project backups"

@projects.each_pair do |id, project|
  puts "    * Backing up #{project[:name]}"

  project_threads = []

  project_threads << Thread.new do
    `#{ mongodump } --collection #{ project[:classifications] } --out project_dumps/#{ project[:output] }`
  end

  project_threads << Thread.new do
    `#{ mongodump } --collection #{ project[:subjects] } --out project_dumps/#{ project[:output] }`
  end

  project_threads << Thread.new do
    `#{ mongodump } --collection #{ project[:groups] } --out project_dumps/#{ project[:output] }`
  end

  project_threads << Thread.new do
    `#{ mongodump } --collection users --out project_dumps/#{ project[:output] } --query '{ "projects.#{ id }": { $exists:true } }'`
    `mv project_dumps/#{ project[:output] }/ouroboros*/users.bson project_dumps/#{ project[:output] }/#{ project[:name] }_users.bson`
    `mv project_dumps/#{ project[:output] }/ouroboros*/users.metadata.json project_dumps/#{ project[:output] }/#{ project[:name] }_users.metadata.json`
  end

  sleep 1
  project_threads.map &:join

  Dir["project_dumps/#{ project[:output] }/ouroboros*/*"].each do |file|
    `mv #{ file } project_dumps/#{ project[:output] }/`
  end

  `rm -rf project_dumps/#{ project[:output] }/ouroboros*`
  `cd project_dumps; tar czvf #{ project[:output] }.tar.gz #{ project[:output] }`
  `rm -rf project_dumps/#{ project[:output] }`
  `mv project_dumps/#{ project[:output] }.tar.gz backups/ouroboros_projects/#{ project[:output] }.tar.gz`

  path = "ouroboros_projects/#{ project[:output] }.tar.gz"
  upload project[:name].titleize, path, "backups/#{ path }", id
end

`rm -rf project_dumps`

puts "* Starting complete Ouroboros backup"

`#{ mongodump } --out ouroboros_#{ @timestamp }`

Dir["ouroboros_#{ @timestamp }/ouroboros*/*"].each do |file|
  next if File.basename(file) =~ /_cache/
  `mv #{ file } ouroboros_#{ @timestamp }/`
end

`rm -rf ouroboros_#{ @timestamp }/ouroboros*`
complete_file_name = "ouroboros_#{ @timestamp }.tar.gz"
`tar czvf #{ complete_file_name } ouroboros_#{ @timestamp }`
`mv #{ complete_file_name } backups/`

exclusions = [
  '_cache',
  'administrations',
  'administrators',
  'data_requests',
  'jobs',
  'messages',
  'classifications',
  'groups',
  'manifest_entries',
  'manifests',
  'moderations',
  'project_statuses',
  'subjects',
  'translations',
  'user_extra_infos',
  'users'
].collect{ |f| "--exclude '#{ f }.*'" }.join ' '
filtered_file_name = "ouroboros_#{ @timestamp }_filtered.tar.gz"
`tar #{ exclusions } -czvf #{ filtered_file_name } ouroboros_#{ @timestamp }`
`mv #{ filtered_file_name } backups/`

talk_files = %w(boards discussions projects subject_sets).collect{ |f| "ouroboros_#{ @timestamp }/#{ f }.*" }.join ' '
talk_only_file_name = "ouroboros_#{ @timestamp }_talk_only.tar.gz"
`tar czvf #{ talk_only_file_name } #{ talk_files }`
`mv #{ talk_only_file_name } backups/`

`rm -rf ouroboros_#{ @timestamp }`

Dir.chdir 'backups'

puts "    * Uploading complete backup"
complete_upload = upload 'Ouroboros', complete_file_name, complete_file_name
puts "    * Uploading filtered backup"
filtered_upload = upload 'Filtered Ouroboros', filtered_file_name, filtered_file_name
puts "    * Uploading talk backup"
talk_upload = upload 'Talk only', talk_only_file_name, talk_only_file_name

puts "* Sending notification emails"

email = [
  "Ouroboros Backup #{ @timestamp }: 1 complete backup and #{ @projects.length } project backups.",
  complete_upload,
  filtered_upload,
  talk_upload
]

filtered_email = [
  "Ouroboros Backup #{ @timestamp }: 1 complete backup and #{ @projects.length } project backups.",
  filtered_upload,
  talk_upload
].join "\n\n"

@projects.each_pair{ |id, project| email << project[:email_line] }
email = email.join "\n\n"

mail = Mail.new do
  from 'noreply@zooniverse.org'
  to %w(sysadmins@zooniverse.org)
  subject "Ouroboros MongoDB Backup #{ @timestamp }"
  body email
end
mail.deliver!


filtered_recipients = ['sysadmins@zooniverse.org'] + config.fetch('filtered_recipients', [])

filtered_mail = Mail.new do
  from 'team@zooniverse.org'
  to filtered_recipients
  subject "Ouroboros MongoDB Backup #{ @timestamp }"
  body filtered_email
end
filtered_mail.deliver!

config['project_mailings'].each do |id, emails|
  project = @projects[id]

  mail = Mail.new do
    from 'team@zooniverse.org'
    to emails
    cc 'sysadmins@zooniverse.org'
    subject "#{ project[:name] } MongoDB Backup #{ @timestamp }"
    body project[:email_line]
  end
  mail.deliver!
end


puts "* Cleaning up"

`rm -rf #{output_dir}/*`

puts "Backup complete"
