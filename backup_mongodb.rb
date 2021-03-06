require 'active_support/core_ext/string/inflections'
require 'aws-sdk'
require 'mongo'
require 'mail'
require 'yaml'

$config = YAML.load(File.read('/config.yml'))

mongo_host = nil

output_dir = '/out/'

$config['mongo']['hosts'].each do |host|
  secondary_check = <<-SH
    mongo --quiet #{host}/admin --eval "
    db.auth('#{$config['mongo']['admin']['user']}',
      '#{$config['mongo']['admin']['pass']}');
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

Aws.config.update({
  credentials: Aws::Credentials.new($config['aws']['s3']['access_key_id'], $config['aws']['s3']['secret_access_key']),
  region: 'us-east-1'
})
s3 = Aws::S3::Resource.new(signature_version: 'v4')
@bucket = s3.bucket($config['aws']['s3']['bucket'])
@mutex = Mutex.new


Mail.defaults do
  delivery_method(:smtp, {
    enable_starttls_auto: true,
    address: $config['aws']['ses']['address'],
    port: $config['aws']['ses']['port'],
    domain: $config['aws']['ses']['domain'],
    authentication: :plain,
    user_name: $config['aws']['ses']['user_name'],
    password: $config['aws']['ses']['password']
  })
end

@timestamp = Time.now.strftime '%Y-%m-%d'
Dir.chdir output_dir
Dir.mkdir 'backups'
Dir.mkdir "backups/ouroboros_projects"
Dir.mkdir "backups/standalone_projects"

connection = Mongo::ReplSetConnection.new $config['mongo']['hosts'], { name: $config['mongo']['ouroboros']['rs_name'] }
mongo = connection[$config['mongo']['ouroboros']['db_name']]
mongo.authenticate $config['mongo']['ouroboros']['user'], $config['mongo']['ouroboros']['pass']

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

mongodump = "mongodump --host #{mongo_host} --db #{$config['mongo']['ouroboros']['db_name']} --username #{$config['mongo']['ouroboros']['user']} --password #{$config['mongo']['ouroboros']['pass']}"
mongodump_staging = "mongodump --host #{$config['mongo']['staging']['host']} --db #{$config['mongo']['staging']['db_name']}"
mongoexport = "mongoexport --host #{mongo_host} --db #{$config['mongo']['ouroboros']['db_name']} --username #{$config['mongo']['ouroboros']['user']} --password #{$config['mongo']['ouroboros']['pass']}"
sandboxmongoexport = "mongoexport --host #{$config['mongo']['sandbox']['host']} --db #{$config['mongo']['sandbox']['db_name']} --username #{$config['mongo']['sandbox']['user']} --password #{$config['mongo']['sandbox']['pass']}"

def upload(name, path, file_path, id = nil)
  dump_object = @bucket.object("#{$config['aws']['s3']['prefix']}#{ @timestamp }/#{ path }")
  dump_object.upload_file file_path, {server_side_encryption: 'aws:kms'}
  url = dump_object.presigned_url(:get, expires_in: 604800)
  file_size = `stat -c %s #{ file_path }`

  if id
    @mutex.synchronize do
      @projects[id][:email_line] = "Backed up #{ name } (#{ '%.3f' % (file_size.to_f / 1048576)}) MB (#{ url })"
    end
  else
    "Backed up #{ name } (#{ '%.3f' % (file_size.to_f / 1048576)}) MB (#{ url })"
  end
end

puts "* Starting standalone backups"

$config.fetch('standalone_projects', {}).each_pair do |name, h|
  @projects[name] = {}
  puts "    * Backing up #{name}"

  `mkdir -p standalone_dumps/#{name}`
  `mongodump --excludeCollectionsWithPrefix=system --host #{h['host']}:#{h['port']} --db #{h['database']} -u #{h['username']} -p #{h['password']} --out standalone_dumps/#{name}/`

  `cd standalone_dumps; tar czvf #{name}.tar.gz #{name}`
  `mv standalone_dumps/#{name}.tar.gz backups/standalone_projects/#{name}.tar.gz`

  path = "standalone_projects/#{name}.tar.gz"
  upload name.titleize, path, "backups/#{ path }", name
  project = @projects[name]

  exclusions = h.fetch('sanitized_excludes', []).collect{
    |f| "--exclude '#{ f }.*'"
  }.join ' '

  if exclusions != ""
    project.delete :email_line
    @projects["#{ name }_sanitized"] = {}
    `cd standalone_dumps; tar -czv #{exclusions} -f #{name}_sanitized.tar.gz #{name}`
    `mv standalone_dumps/#{name}_sanitized.tar.gz backups/standalone_projects/#{name}_sanitized.tar.gz`
    path = "standalone_projects/#{name}_sanitized.tar.gz"
    upload name.titleize, path, "backups/#{ path }", "#{ name }_sanitized"
    project = @projects["#{ name }_sanitized"]
  end

  `rm -rf standalone_dumps/#{name}`

  emails = h.fetch('email_recipients', [])
  emails.push('sysadmins@zooniverse.org')

  mail = Mail.new do
    from 'team@zooniverse.org'
    to emails
    subject "#{name} MongoDB Backup #{ @timestamp }"
    body project[:email_line]
  end
  mail.deliver!
  project.delete :email_line
  @projects.delete name
  @projects.delete "#{ name }_sanitized"
end

sanitized_subject_fields = %w(activated_at classification_count coords created_at group group_id location metadata project_id random state updated_at workflow_ids zooniverse_id)
sanitized_classification_fields = %w(annotations created_at project_id subject_ids subjects tutorial updated_at user_id user_name workflow_id)
sanitized_group_fields = %w(categories classification_count created_at metadata name project_id project_name random state stats subjects updated_at zooniverse_id)

puts "* Starting sanitized backups"

$config['sanitized_projects'].each_pair do |id, emails|
  project = @projects[id]

  puts "    * Backing up #{project[:name]}"

  sanitized_project_threads = []
  sanitized_output = "sanitized_#{ project[:output] }"

  `mkdir -p project_dumps/#{ sanitized_output }`

  if $config['sandbox_projects'].include? id
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

  sanitized_project_threads << Thread.new do
    `#{ export_cmd } --collection projects -q '{_id: ObjectId("#{id}")}' --out project_dumps/#{ sanitized_output }/projects.json`
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

`#{ mongodump_staging } --out ouroboros_staging_#{ @timestamp }`
staging_file_name = "ouroboros_staging_#{ @timestamp }.tar.gz"
`tar czvf #{ staging_file_name } ouroboros_staging_#{ @timestamp }`
`mv #{ staging_file_name } backups/`
`rm -rf ouroboros_staging_#{ @timestamp }`

Dir.chdir 'backups'

puts "    * Uploading complete backup"
complete_upload = upload 'Ouroboros', complete_file_name, complete_file_name
puts "    * Uploading filtered backup"
filtered_upload = upload 'Filtered Ouroboros', filtered_file_name, filtered_file_name
puts "    * Uploading talk backup"
talk_upload = upload 'Talk only', talk_only_file_name, talk_only_file_name
puts "    * Uploading staging backup"
staging_upload = upload 'Staging', staging_file_name, staging_file_name

puts "* Sending notification emails"

email = [
  "Ouroboros Backup #{ @timestamp }: 1 complete backup.",
  complete_upload,
  filtered_upload,
  talk_upload,
  staging_upload
]

filtered_email = [
  "Ouroboros Backup #{ @timestamp }: 1 complete backup.",
  filtered_upload,
  talk_upload
].join "\n\n"

email = email.join "\n\n"

mail = Mail.new do
  from 'noreply@zooniverse.org'
  to %w(sysadmins@zooniverse.org)
  subject "Ouroboros MongoDB Backup #{ @timestamp }"
  body email
end
mail.deliver!

filtered_recipients = ['sysadmins@zooniverse.org'] + $config.fetch('filtered_recipients', [])

filtered_mail = Mail.new do
  from 'team@zooniverse.org'
  to filtered_recipients
  subject "Ouroboros MongoDB Backup #{ @timestamp }"
  body filtered_email
end
filtered_mail.deliver!

puts "* Cleaning up"

`rm -rf #{output_dir}/*`

puts "Backup complete"
