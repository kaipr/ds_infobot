require 'rubygems'
gem 'dm-core'
require 'dm-core'
require 'dm-aggregates'

require 'cgi'
require 'iconv'
#require 'net/http'
require 'open-uri'
require 'zlib'

#DataMapper::Logger.new('/home/kai/dmlog.log', :debug)
DataMapper.setup(:default, 'postgres://kai:kai@localhost/dsdata')

class Village
  include DataMapper::Resource
  
  storage_names[:default] = 'villages'
  
  property :server,     String, :length => 5, :serial => true
  property :ds_id,      Integer, :serial => true
  property :name,       String, :length => 32
  property :x,          Integer
  property :y,          Integer
  property :player_dsid,Integer
  property :points,     Integer
  property :rank,       Integer
  
  def player
    return nil if self.player_dsid == 0
    @player ||= Player.first(:ds_id => self.player_dsid, :server => self.server)
  end
  
end

class Ally
  include DataMapper::Resource
  
  storage_names[:default] = 'allies'
  
  property :server,     String, :length => 5, :serial => true
  property :ds_id,      Integer, :serial => true
  property :name,       String, :length => 32
  property :tag,        String, :length => 6
  property :members,    Integer
  property :villages,   Integer
  property :points,     Integer
  property :all_points, Integer
  property :rank,       Integer
  
  def players
    Player.all :ally_dsid => self.ds_id
  end
  
  def players_ids
    @players_ids ||= self.players.map {|p| p.ds_id }
  end
  
  def won_conquers(limit = 0)
    Conquer.all :new_player_dsid.in => players_ids, :server => self.server, :order => [:conquered_at.desc], :limit => limit
  end
  
  def lost_conquers(limit = 0)
    Conquer.all :old_player_dsid.in => players_ids, :server => self.server, :order => [:conquered_at.desc], :limit => limit
  end
  
  def won_conquers_count(since_date = nil)
    since_date_s = since_date.nil? ? '' : " AND c.conquered_at BETWEEN '#{since_date}'::timestamp AND LOCALTIMESTAMP"
    @won_conquers_count ||= repository.adapter.query("SELECT COUNT(c.*) FROM conquers AS c, players AS p WHERE p.ally_dsid = #{ds_id} AND c.new_player_dsid = p.ds_id AND c.server = '#{server}'#{since_date_s}")[0]
  end
  
  def lost_conquers_count(since_date = nil)
    since_date_s = since_date.nil? ? '' : " AND c.conquered_at BETWEEN '#{since_date}'::timestamp AND LOCALTIMESTAMP"
    @lost_conquers_count ||= repository.adapter.query("SELECT COUNT(c.*) FROM conquers AS c, players AS p WHERE p.ally_dsid = #{ds_id}  AND c.old_player_dsid = p.ds_id AND c.server = '#{server}'#{since_date_s}")[0]
  end
  
  def nonplayer_conquers_count(since_date = nil)
    since_date_s = since_date.nil? ? '' : " AND c.conquered_at BETWEEN '#{since_date}'::timestamp AND LOCALTIMESTAMP"
    @nonplayer_conquers_count ||= repository.adapter.query("SELECT COUNT(c.*) FROM conquers AS c, players AS p WHERE p.ally_dsid = #{ds_id} AND c.new_player_dsid = p.ds_id AND c.old_player_dsid = 0 AND c.server = '#{server}'#{since_date_s}")[0]
  end
  
  def self_conquers_count(since_date = nil)
    since_date_s = since_date.nil? ? '' : " AND c.conquered_at BETWEEN '#{since_date}'::timestamp AND LOCALTIMESTAMP"
    @self_conquers_count ||= repository.adapter.query("SELECT COUNT(c.*) FROM conquers AS c, players AS p WHERE p.ally_dsid = #{ds_id} AND c.new_player_dsid = p.ds_id AND c.old_player_dsid = p.ds_id AND c.server = '#{server}'#{since_date_s}")[0]
  end
  
  def nonplayer_conquers_rate(since_date = nil)
    if self.won_conquers_count(since_date) > 0
      return ((self.nonplayer_conquers_count(since_date) * 1.0) / self.won_conquers_count(since_date) * 100).round
    else
      return 0
    end
  end
  
end

class Player
  include DataMapper::Resource
  
  storage_names[:default] = 'players'
  
  property :server,     String, :length => 5, :serial => true
  property :ds_id,      Integer, :serial => true
  property :name,       String, :length => 24
  property :ally_dsid,  Integer
  property :villages,   Integer
  property :points,     Integer
  property :rank,       Integer
  property :bash_off_rank,    Integer
  property :bash_off_points,  Integer
  property :bash_def_rank,    Integer
  property :bash_def_points,  Integer
  property :bash_all_rank,    Integer
  property :bash_all_points,  Integer
  
  def ally
    return nil if self.ally_dsid == 0
    @ally ||= Ally.first(:ds_id => self.ally_dsid, :server => self.server)
  end
  
  def bash_type
    @bash_type ||= self.bash_off_points > self.bash_def_points ? "Offer(in)" : "Deffer(in)"
  end
  
  def won_conquers(limit = 0)
    Conquer.all :new_player_dsid => self.ds_id, :server => self.server, :order => [:conquered_at.desc], :limit => limit
  end
  
  def lost_conquers(limit = 0)
    Conquer.all :old_player_dsid => self.ds_id, :server => self.server, :order => [:conquered_at.desc], :limit => limit
  end
  
  def won_conquers_count(since_date = nil)
    since_date_s = since_date.nil? ? '' : " AND c.conquered_at BETWEEN '#{since_date}'::timestamp AND LOCALTIMESTAMP"
    @won_conquers_count ||= repository.adapter.query("SELECT COUNT(c.*) FROM conquers AS c WHERE c.new_player_dsid = #{ds_id} AND server = '#{server}'#{since_date_s}")[0]
  end
  
  def lost_conquers_count(since_date = nil)
    since_date_s = since_date.nil? ? '' : " AND c.conquered_at BETWEEN '#{since_date}'::timestamp AND LOCALTIMESTAMP"
    @lost_conquers_count ||= repository.adapter.query("SELECT COUNT(c.*) FROM conquers AS c WHERE c.old_player_dsid = #{ds_id} AND server = '#{server}'#{since_date_s}")[0]
  end
  
  def nonplayer_conquers_count(since_date = nil)
    since_date_s = since_date.nil? ? '' : " AND c.conquered_at BETWEEN '#{since_date}'::timestamp AND LOCALTIMESTAMP"
    @nonplayer_conquers_count ||= repository.adapter.query("SELECT COUNT(c.*) FROM conquers AS c WHERE c.new_player_dsid = #{ds_id} AND c.old_player_dsid = 0 AND server = '#{server}'#{since_date_s}")[0]
  end
  
  def self_conquers_count(since_date = nil)
    since_date_s = since_date.nil? ? '' : " AND c.conquered_at BETWEEN '#{since_date}'::timestamp AND LOCALTIMESTAMP"
    @self_conquers_count ||= repository.adapter.query("SELECT COUNT(c.*) FROM conquers AS c WHERE c.new_player_dsid = #{ds_id} AND c.old_player_dsid = #{ds_id} AND server = '#{server}'#{since_date_s}")[0]
  end
  
  def nonplayer_conquers_rate(since_date = nil)
    if self.won_conquers_count(since_date) > 0
      return ((self.nonplayer_conquers_count(since_date) * 1.0) / self.won_conquers_count(since_date) * 100).round
    else
      return 0
    end
  end
  
end

class Conquer
  include DataMapper::Resource
  
  storage_names[:default] = 'conquers'
  
  property :id,         Integer, :serial => true
  property :server,     String, :length => 5
  
  property :village_dsid,     Integer
  property :conquered_at,     DateTime
  property :new_player_dsid,  Integer
  property :old_player_dsid,  Integer
  
  def village
    @village ||= Village.first(:ds_id => self.village_dsid, :server => self.server)
  end
  
  def new_player
    @new_player ||= Player.first(:ds_id => self.new_player_dsid, :server => self.server)
  end
  
  def old_player
    @old_player ||= Player.first(:ds_id => self.old_player_dsid, :server => self.server)
  end
  
end

class ChannelSetting
  include DataMapper::Resource
  
  storage_names[:default] = 'channel_settings'
  
  property :id,         Integer, :serial => true
  property :bot_id,     Integer
  property :channel,    String
  property :channel_pw, String
  property :server,     String, :length => 5
  property :ally_dsid,  Integer
  property :auto_village, Integer, :default => 0
  property :usage_count, Integer
  property :created_at, DateTime
  property :updated_at, DateTime
  property :user_id,    Integer
  
  def auto_village_name
    case self.auto_village.to_i
    when 0:
      "aus"
    when 1:
      "nur Link"
    when 2:
      "nur Info"
    when 3:
      "Info + Link"
    end
  end
  
  def ticker_settings
    TickerSetting.all :channel_setting_id => self.id
  end
  
end

class TickerSetting
  include DataMapper::Resource
  
  storage_names[:default] = 'ticker_settings'
  
  property :id,         Integer, :serial => true
  property :channel_setting_id, Integer
  property :server,     String, :length => 5
  property :continent,  String
  property :ally,       String
  property :created_at, DateTime
  property :updated_at, DateTime
  property :ally_relation, Integer
  
  def channel
    @channel_name ||= self.channel_setting.channel
  end
  
  def channel_setting
    ChannelSetting.first :id => self.channel_setting_id
  end
  
end

class WhoisPlayerInfo
  include DataMapper::Resource
  
  storage_names[:default] = 'whois_player_infos'
  
  property :id,         Integer, :serial => true
  property :name,       String
  property :player_dsid,  Integer
  property :server,     String, :length => 5
  property :created_at, DateTime
  
  def player
    opts = {:ds_id => self.player_dsid}
    opts.merge({:server => self.server}) unless self.server.nil?
    @player ||= Player.first(opts)
  end
  
end

#######################################################################
#                               Import
#######################################################################

class WorldDataImport
  
  def init(server, debug = [], bot = false, fetch_files = true)
    @server       = server
    @server_url   = "http://#{@server}.die-staemme.de/"
    @debug        = [:error, :main_debug]#debug
    @bot          = bot
    @fetch_files  = fetch_files
    @current_time = Time.now
    @query_queue_s = ''
    @query_queue_temp = {}
    @querys_in_queue = 0
  end
  
  def import_all
    import_villages
    import_allies
    import_players
    #import_conquers
  end
  
  def get_file(file_name)
    file_path = "/home/kai/rbot_temp/#{@server}_#{file_name}"
    if @fetch_files
      file = File.open(file_path, File::WRONLY|File::TRUNC|File::CREAT)
      file.puts open("#{@server_url}map/#{file_name}").read
      file.close
    end
    file = open(file_path, 'r')
  end
  
  def import_villages
    DataMapper.repository(:default).adapter.execute("DELETE FROM villages WHERE server = '#{@server}';")
    vacuum_table('villages')
    
    debug_msg "Import villages, loading file..."
    debug_timer
    
    begin
      file = get_file('village.txt.gz')
      gz = Zlib::GzipReader.new(file)
      debug_msg "Village file loaded after #{debug_timer} seconds, importing..."
      #DataMapper.repository(:default).adapter.execute('BEGIN;')
      gz.each_line do |l|
        raw = l.split(',')
      
        raw[1] = unescape(raw[1])
      
        query_queue "INSERT INTO villages (server, ds_id, name, x, y, player_dsid, points, rank) VALUES", "('#{@server}', #{raw[0].to_i}, '#{raw[1].to_s[0...32]}', #{raw[2].to_i}, #{raw[3].to_i}, #{raw[4].to_i}, #{raw[5].to_i}, #{raw[6].to_i})"
      end
      gz.close
      
      #DataMapper.repository(:default).adapter.execute('COMMIT;')
      analyze_table('villages')
    rescue Exception => e
      #DataMapper.repository(:default).adapter.execute('ROLLBACK;')
      debug_msg "Error while importing villages: #{e.inspect}", :error
    end
    
    duration = self.debug_timer
    debug_msg "Import villages finished after #{duration} seconds!", :main_debug
    duration
  end
  
  def import_allies
    DataMapper.repository(:default).adapter.execute("DELETE FROM allies WHERE server = '#{@server}';")
    vacuum_table('allies')
    
    debug_msg "Import allies, loading file..."
    debug_timer
    
    begin
      file = get_file('ally.txt.gz')
      gz = Zlib::GzipReader.new(file)
      debug_msg "Ally file loaded after #{debug_timer} seconds, importing..."
      DataMapper.repository(:default).adapter.execute('BEGIN;')
      gz.each_line do |l|
        raw = l.split(',')
        raw[1] = unescape(raw[1])
        raw[2] = unescape(raw[2])
      
        query_queue "INSERT INTO allies (server, ds_id, name, tag, members, villages, points, all_points, rank) VALUES", "('#{@server}', #{raw[0].to_i}, '#{raw[1].to_s[0..31]}', '#{raw[2].to_s[0..5]}', #{raw[3].to_i}, #{raw[4].to_i}, #{raw[5].to_i}, #{raw[6].to_i}, #{raw[7].to_i})"
      end
      gz.close
      
      run_query_queue
      DataMapper.repository(:default).adapter.execute('COMMIT;')
      analyze_table('allies')
    rescue Exception => e
      DataMapper.repository(:default).adapter.execute('ROLLBACK;')
      debug_msg "Error while importing allies: #{e.inspect}", :error
    end
    
    duration = self.debug_timer
    debug_msg "Import allies finished in #{duration} seconds!", :main_debug
    duration
  end
  
  def import_players
    debug_msg "Import players...loading files..."
    players = {}
    debug_timer
    
    begin
      file = get_file('tribe.txt.gz')
      gz = Zlib::GzipReader.new(file)
      gz.each_line do |l|
        raw = l.split(',')
        raw[1] = unescape(raw[1])
        players[raw[0].to_i] = {}
        players[raw[0].to_i][:name] = raw[1].to_s[0..23]
        players[raw[0].to_i][:ally_dsid] = raw[2].to_i
        players[raw[0].to_i][:villages] = raw[3].to_i
        players[raw[0].to_i][:points] = raw[4].to_i
        players[raw[0].to_i][:rank] = raw[5].to_i
        players[raw[0].to_i][:bash_off_rank] = 0
        players[raw[0].to_i][:bash_off_points] = 0
        players[raw[0].to_i][:bash_def_rank] = 0
        players[raw[0].to_i][:bash_def_points] = 0
        players[raw[0].to_i][:bash_all_rank] = 0
        players[raw[0].to_i][:bash_all_points] = 0
      end
      gz.close
    
      file = get_file('kill_att.txt.gz')
      gz = Zlib::GzipReader.new(file)
      gz.each_line do |l|
        raw = l.split(',')
        unless players[raw[1].to_i].nil?
          players[raw[1].to_i][:bash_off_rank] = raw[0].to_i
          players[raw[1].to_i][:bash_off_points] = raw[2].to_i
        end
      end
      gz.close
    
      file = get_file('kill_def.txt.gz')
      gz = Zlib::GzipReader.new(file)
      gz.each_line do |l|
        raw = l.split(',')
        unless players[raw[1].to_i].nil?
          players[raw[1].to_i][:bash_def_rank] = raw[0].to_i
          players[raw[1].to_i][:bash_def_points] = raw[2].to_i
        end
      end
      gz.close
    
      file = get_file('kill_all.txt.gz')
      gz = Zlib::GzipReader.new(file)
      gz.each_line do |l|
        raw = l.split(',')
        unless players[raw[1].to_i].nil?
          players[raw[1].to_i][:bash_all_rank] = raw[0].to_i
          players[raw[1].to_i][:bash_all_points] = raw[2].to_i
        end
      end
      gz.close
    rescue Exception => e
      debug_msg "Error while loading player files: #{e.inspect}", :error
    end
    
    #unless player_count == players.size
      # TODO: Fragen in welcher Reihenfolge die Spieler exportiert werden, sind neue Spieler 
      # immer hinten? Dann müsste man nicht alle durch gehen
      # Evtl gibts auch INSERT OR UPDATE oder ähnliches in pgsql? Dann einfach nur eine Methode für neue & alte
    #  insert_players(new_players)
    #end
    
    debug_msg "Player files loaded after #{debug_timer} seconds, importing..."
    
    insert_players(players)
  end
  
  def update_players(players)
    #PostgresError: (sql_state=42601) ERROR:  syntax error at or near "="
    #LINE 2:           ally_dsid = 0, 
    
    debug_msg "Updating players..."
    player_in_db_count = Player.count(:server => @server)
    i = 0
    
    DataMapper.repository(:default).adapter.execute('BEGIN;')
    players.each do |player_dsid, data|
      i += 1
      if i <= player_in_db_count
        query_queue("UPDATE players SET", "(
          ally_dsid = #{data[:ally_dsid]}, 
          villages = #{data[:villages]}, 
          points = #{data[:points]}, 
          rank = #{data[:rank]}, 
          bash_off_rank = #{data[:bash_off_rank]}, 
          bash_off_points = #{data[:bash_off_points]}, 
          bash_def_rank = #{data[:bash_def_rank]}, 
          bash_def_points = #{data[:bash_def_points]}, 
          bash_all_rank = #{data[:bash_all_rank]}, 
          bash_all_points = #{data[:bash_all_points]})
          WHERE ds_id = #{player_dsid} && server = '#{@server}'")
      else
        query_queue "INSERT INTO players (server, ds_id, name, ally_dsid, villages, points, rank, bash_off_rank, bash_off_points, bash_def_rank, bash_def_points, bash_all_rank, bash_all_points) VALUES", "('#{@server}', #{player_dsid}, '#{data[:name]}', #{data[:ally_dsid]}, #{data[:villages]}, #{data[:points]}, #{data[:rank]}, #{data[:bash_off_rank]}, #{data[:bash_off_points]}, #{data[:bash_def_rank]}, #{data[:bash_def_points]}, #{data[:bash_all_rank]}, #{data[:bash_all_points]})"
      end
    end
    run_query_queue
    DataMapper.repository(:default).adapter.execute('COMMIT;')
    
    duration = debug_timer
    debug_msg "Updating players finished in #{duration} seconds!"
    duration
  end
  
  def insert_players(players)
    DataMapper.repository(:default).adapter.execute("DELETE FROM players WHERE server = '#{@server}';")
    vacuum_table('players')
    
    debug_msg "Importing players..."
    debug_timer
    begin
      DataMapper.repository(:default).adapter.execute('BEGIN;')
      players.each do |player_dsid, data|
        query_queue "INSERT INTO players (server, ds_id, name, ally_dsid, villages, points, rank, bash_off_rank, bash_off_points, bash_def_rank, bash_def_points, bash_all_rank, bash_all_points) VALUES", "('#{@server}', #{player_dsid}, '#{data[:name]}', #{data[:ally_dsid]}, #{data[:villages]}, #{data[:points]}, #{data[:rank]}, #{data[:bash_off_rank]}, #{data[:bash_off_points]}, #{data[:bash_def_rank]}, #{data[:bash_def_points]}, #{data[:bash_all_rank]}, #{data[:bash_all_points]})"
      end
      run_query_queue
      DataMapper.repository(:default).adapter.execute('COMMIT;')
      analyze_table('players')
    rescue Exception => e
      DataMapper.repository(:default).adapter.execute('ROLLBACK;')
      debug_msg "Error while importing players: #{e.inspect}", :error
    end
    
    duration = debug_timer
    debug_msg "Importing players finished in #{duration} seconds!", :main_debug
    duration
  end
  
  def delete_conquers
    DataMapper.repository(:default).adapter.execute("DELETE FROM conquers WHERE server = '#{@server}';")
    vacuum_table('conquers')
  end
  
  def import_conquers
    debug_timer
    
    debug_msg "Import conquers from main conquer.txt...loading file..."
    conquers_in_db_count = Conquer.count :server => @server
    i = 0
    
    begin
      file = get_file('conquer.txt.gz')
      gz = Zlib::GzipReader.new(file)
      debug_msg "Conquer file loaded after #{debug_timer} seconds, importing..."
      DataMapper.repository(:default).adapter.execute('BEGIN;')
      gz.each_line do |l|
        i += 1
        if i > conquers_in_db_count
          raw = l.split(',')
          query_queue "INSERT INTO conquers (server, village_dsid, conquered_at, new_player_dsid, old_player_dsid) VALUES", "('#{@server}', #{raw[0].to_i}, '#{Time.at(raw[1].to_i)}', #{raw[2].to_i}, #{raw[3].to_i})"
        end
      end
      gz.close
      
      run_query_queue
      DataMapper.repository(:default).adapter.execute('COMMIT;')
      analyze_table('conquers')
    rescue Exception => e
      DataMapper.repository(:default).adapter.execute('ROLLBACK;')
      debug_msg "Error while importing conquers: #{e.inspect}", :error
    end
    
    duration = self.debug_timer
    debug_msg "Import conquers finished in #{duration} seconds!", :main_debug
    duration
  end
  
  def import_conquers_since(timestamp)
    debug_timer
    @current_time = Time.now
    begin
      #DataMapper.repository(:default).adapter.execute('BEGIN;')
      open("#{@server_url}interface.php?func=get_conquer&since=#{timestamp}") do |file|
        file.each_line do |l|
          raw = l.split(',')
          import_conquer_if_new(raw)
        end
      end
      #DataMapper.repository(:default).adapter.execute('COMMIT;')
    rescue Exception => e
      #DataMapper.repository(:default).adapter.execute('ROLLBACK;')
      debug_msg "Error while importing conquers_since: #{e.inspect}", :error
    end
    debug_timer
  end
  
  def import_conquer_if_new(raw)
    conq = {:server => @server, :village_dsid => raw[0].to_i, :conquered_at => Time.at(raw[1].to_i), :new_player_dsid => raw[2].to_i, :old_player_dsid => raw[3].to_i}
    if Conquer.first(conq).nil?
      #DataMapper.repository(:default).adapter.execute("INSERT INTO conquers (server, village_dsid, conquered_at, new_player_dsid, old_player_dsid) VALUES ('#{@server}', #{conq[:village_dsid]}, '#{conq[:conquered_at]}', #{conq[:new_player_dsid]}, #{conq[:old_player_dsid]})")
      Conquer.create(conq)
      v = Village.first(:ds_id => conq[:village_dsid], :server => @server)
      v.update_attributes(:player_dsid => conq[:new_player_dsid]) unless v.nil?
    end
  end
  
  def query_queue(query_begin, values)
    if query_begin[0..5] == 'INSERT'
      if @query_queue_temp[query_begin].nil?
        @query_queue_temp[query_begin] = [values] 
      else
        @query_queue_temp[query_begin] << values
        if @query_queue_temp[query_begin].size >= 20
          @query_queue_s += query_begin + ' ' + @query_queue_temp[query_begin].join(', ') + ';'
          @query_queue_temp.delete query_begin
        end
      end
    else # UPDATE ...
      @query_queue_s+= "#{query_begin} #{values};"
    end
    @querys_in_queue += 1
    run_query_queue if @querys_in_queue >= 8000
  end
  
  def run_query_queue
    if @query_queue_temp.size > 0
      add = @query_queue_temp.map { |b,v| b + ' ' + v.join(', ') + ';' }.join
      @query_queue_s += add
      @query_queue_temp = {}
    end
    @ic ||= Iconv.new('UTF-8//IGNORE', 'UTF-8')
    query = @ic.iconv(@query_queue_s + ' ')[0..-2]
    query.gsub!('?', ' ') # TODO!!!
    sleep(0.3)
    DataMapper.repository(:default).adapter.execute(query)
    @query_queue_s= ''
    @querys_in_queue = 0
  end
  
  def vacuum_table(table)
    DataMapper.repository(:default).adapter.execute("VACUUM #{table};")
    sleep(0.5)
  end
  
  def analyze_table(table)
    DataMapper.repository(:default).adapter.execute("ANALYZE #{table};")
    sleep(0.5)
  end
  
  def vacuum_full_analyze_db
    DataMapper.repository(:default).adapter.execute("VACUUM FULL ANALYZE;")
    sleep(2)
  end
  
  def debug_msg(msg, level = :debug)
    if !@debug.empty? && @debug.include?(level)
      msg = "[#{@server}]: #{msg}"
      if @bot
        @bot.say '#dsbot-status', msg
      else
        puts msg
      end
    end
  end
  
  def debug_timer
    if @last_time
      duration = Time.now.to_i - @last_time
      @last_time = Time.now.to_i
      return duration
    else
      @last_time = Time.now.to_i
      return 0
    end
  end
  
  def unescape(string)
    return "" if string.nil?
    CGI.unescapeHTML(CGI.unescape(string.to_s)).gsub('\'', '\'\'') #.gsub(/['"\\\x0]/,'\\\\\0')
  end
  
end







#######################################################################
#                                 DS
#######################################################################






class DieStaemmePlugin < Plugin
  
  Config.register Config::IntegerValue.new('ds.bot_id',
    :default => 0,
    :desc => "Bot id")
  
  Config.register Config::BooleanValue.new('ds.auto_join_channels',
    :default => false,
    :desc => "Alle per ds plugin konfigurierten Channel beim Start automatisch joinen")
  Config.register Config::BooleanValue.new('ds.ticker_active',
    :default => false,
    :desc => "Timer für Adelticker ausführen ja/nein")
  Config.register Config::IntegerValue.new('ds.ticker_period',
    :default => 30,
    :desc => "Periode in Sekunden in der die neuen Adelungen ausgegeben werden")
  
  Config.register Config::BooleanValue.new('ds.auto_server_import',
    :default => false,
    :desc => "Automatischer Import aller konfigurierten Server")
  Config.register Config::ArrayValue.new('ds.import_servers',
    :default => [],
    :desc => "Server die importiert werden")
  Config.register Config::IntegerValue.new('ds.server_import_period',
    :default => 70,
    :desc => "Periode in Minuten in der der Serverimport ausgeführt wird")
  Config.register Config::BooleanValue.new('ds.conquer_import',
    :default => false,
    :desc => "Timer für Adel import ausführen ja/nein")
  Config.register Config::IntegerValue.new('ds.conquer_import_period',
    :default => 60,
    :desc => "Periode in Sekunden in der die neuen Adelungen abgefragt werden")
  
  def initialize
    super

    @bot_id = @bot.config['ds.bot_id']
    
    load_channel_settings
    
    # spam protection
    @last_actions = {}
    @last_actions[:channel] = {}
    @last_actions[:user]    = {}
    
    @timers = {}
    
    @import_run = 0
    
    if @bot.config['ds.conquer_import']
      start_conquer_import_timer
    end
    
    if @bot.config['ds.auto_server_import']
      start_server_import_timer
    end
    
    if @bot.config['ds.ticker_active']
      start_ticker_timer
    end
  end
  
  def start_server_import_timer
    period = @bot.config['ds.server_import_period'].to_i * 60
    @timers[:server_import] = @bot.timer.add(period) do
      stop_conquer_import_timer
      start = Time.now.to_i
      what = (@import_run % 5 == 0) ? 'normal' : 'min'
      import_data(@bot.config['ds.import_servers'], what, [:debug, :error])
      @import_run += 1
      duration = Time.now.to_i - start
      @conquer_import_seconds_back = duration + 90
      start_conquer_import_timer
    end
  end
  
  def stop_server_import_timer
    @bot.timer.remove(@timers[:server_import])
  end
  
  def start_conquer_import_timer
    @import_run_num = 1
    @conquer_import_period = @bot.config['ds.conquer_import_period']
    @conquer_import_seconds_back = 90 unless @conquer_import_seconds_back.to_i > 90 # TODO: Sauberer lösen
    
    @timers[:conquer_import] = @bot.timer.add(@conquer_import_period) do
      @bot.config['ds.import_servers'].each do |server|
        w = WorldDataImport.new
        w.init(server, [:error], @bot)
        
        #if @import_run_num == 1 
        #  @conquer_import_seconds_back = 600
        #else
        #  @conquer_import_seconds_back = 90
        #end
        w.import_conquers_since(Time.now.to_i - (@conquer_import_period + @conquer_import_seconds_back))
        w = nil
      end
      @import_run_num += 1
      @conquer_import_seconds_back = 90 # TODO: Sauberer lösen
    end
  end
  
  def stop_conquer_import_timer
    @bot.timer.remove(@timers[:conquer_import])
  end
  
  def start_ticker_timer
    @channel_setting_ids = @channel_settings.map {|c,s| s.id}
    @last_conquer_id = Conquer.max(:id)
    #load_ticker_settings
    
    @timers[:ticker] = @bot.timer.add(@bot.config['ds.ticker_period']) do
      load_ticker_settings
      sended_conquer = {}
      Conquer.all(:id.gt => @last_conquer_id).each do |conquer|
        @last_conquer_id = conquer.id
        
        @ticker_settings.each do |ticker_setting|
          #begin
            if (ticker_setting.server == conquer.server) && !sended_conquer[ticker_setting.channel_setting_id]
      
              matches = false
              new_p = false
              old_p = false
              color = false
              if !ticker_setting.ally.blank?
                new_p = (!conquer.new_player.ally.nil? && conquer.new_player.ally.tag == ticker_setting.ally)
                old_p = (!conquer.old_player.nil? && !conquer.old_player.ally.nil? && conquer.old_player.ally.tag == ticker_setting.ally)
                matches = new_p || old_p
              end
              if !ticker_setting.continent.blank? && (matches || ticker_setting.ally.blank?)
                matches = coordinates_on_continent?(conquer.village.x, conquer.village.y, ticker_setting.continent)
              end
      
              if matches
                # ally relation:
                # 0: eigener stamm
                # 1: allied stamm
                # 2: nap stamm
                # 3: feind stamm
                if !ticker_setting.ally_relation.blank?
                  if (conquer.new_player_dsid == conquer.old_player_dsid) || (new_p && old_p)
                    color = :olive
                  else
                    case ticker_setting.ally_relation
                    when 0
                      new_p ? color = :green : nil
                      old_p ? color = :red : nil
                    when 1
                      new_p ? color = :royal_blue : nil
                      old_p ? color = :red : nil
                    when 2
                      color = :purple
                    when 3
                      new_p ? color = :red : nil
                      old_p ? color = :green : nil
                    end
                  end
                end
                color = color ? "#{Color}#{ColorCode[color]} " : ''
                color_end = color ? "#{Color}" : ''
                @bot.say ticker_setting.channel, color + format_conquer(conquer) + color_end
                sended_conquer[ticker_setting.channel_setting_id] = true
              end
            end
          #rescue Exception
          #end
        end
        sended_conquer = {}
      end
    end
  end
  
  def stop_ticker_timer
    @bot.timer.remove(@timers[:ticker])
  end
  
  def cleanup
    @timers.each do |name, timer|
      @bot.timer.remove(timer)
    end
  end
  
  def connect
    if @bot.config['ds.auto_join_channels']
      @bot.timer.add_once(15) do
        @channel_settings.each do |channel, chsetting|
          join_channel(channel, chsetting.channel_pw)
        end
      end
    end
  end

  def help(plugin, topic="")
    case (topic.to_sym rescue nil)
    when :spieler
      "spieler [spielername] - Gibt Informationen zum Spieler mit dem Namen [spielername] aus. Wird keine exakte Übereinstimmung mit einem Spieler oder einer whois-info gefunden werden Namen gesucht die diesen enthalten und angezeigt (wenn es nur einen gibt wird der Spieler dazu angezeigt) Beispiel: +spieler Irgendwer"
    when :stamm, :stamminfo
      "stamm [stammtag] - Gibt Informationen zum Stamm mit dem Tag [stammtag] aus. Beispiel: +stamm -FooBar-"
    when :dorf, :dorfinfo
      "dorf [xxx|yyy] - Gibt Informationen zum Dorf mit den Koordinaten [xxx|yyy] aus. Je nach Einstellung werden Dorfinfos auch ausgegeben, wenn sie irgendwo im Text stehen (ohne direkten Aufruf des dorf commands). Beispiel: +dorf 234|567"
    when :dorfadelungen
      "dorfadelungen [xxx|yyy] - Gibt die letzten 2 Adelungen des Dorfes mit den Koordinaten [xxx|yyy] aus (sofern vorhanden). Beispiel: +dorfadelungen 234|567"
    when :adel, :adel
      "adel [name] [in [tage] tagen] - Gibt Informationen, wie der Spieler/Stamm [name] (für Stamm das Stammtag benutzen!) heute oder in den letzten [tage] Tagen geadelt hat. Die Angabe von Tagen ist optional. Beispiel: +adel Irgendwer in 7 tagen"
    when :adelungen
      "adelungen [name] [hat [verloren|gewonnen]] - Listet die letzten 3 Adelungen des Spielers/Stammes [name] auf. Als Default die gewonnenen, mit dem Zusatz 'hat verloren' können die verlorenen abgefragt werden. Beispiel: +adelungen Irgendwer hat verloren"
    when :dschannelinfo
      "dschannelinfo - Gibt Informationen über die Botkonfiguration des aktuellen Channels aus. Mit 'dschannelinfo ticker' kann man die Adelticker-Einstellungen abfragen."
    when :antrag
      "Du willst auch eine mausii in deinem Channel haben? Vorraussetzung ist ein Q und ein paar aktive User im Channel zu haben, dann eine Anfrage an Katos stellen und folgendes angeben: 1) Den Channel in den mausii soll 2) Channel Passwort (wenn vorhanden) 3) Den DS Server der für den Channel als Standard definiert werden soll 4) Automatische Dorfinfo bei Koordinaten im Fließtext (0:keine, 1:link, 2:info, 3:info+link) 5) Gewünschte Adelticker (die Tags der Stämme angeben, bitte auf richtige Schreibweise achten). Wenn Katos nicht reagiert, per Query anschreiben (nicht spamen)."
    else
      "Verfügbare Befehle: spieler, stamm, dorf, dorfadelungen, adel, adelungen, dschannelinfo (details mit \"hilfe [befehl]\" abfragen). Mit dem Anhang \"auf server xx\" kann man andere Server abfragen, Standard ist der für den Channel eingestellte. Spamschutz erlaubt alle 2 Sekunden Abfragen im Channel, alle 5 Sekunden von einem User. Weitere Hilfe folgt. Hilfechannel: #dsbot"
    end
  end
  
  def hilfe(m, params)
    cmd = params[:cmd].to_s
    m.reply help('diestaemme', cmd)
  end
  
  def handle_activate_ticker(m, params)
    channel = params[:channel].to_s
    what    = params[:what].to_s
    server  = get_server(m, params)
    which   = params[:which].to_s
    
    if what == 'kontinent' 
      filter_type = 'continent'
      msg = "Kontinent #{which}"
    else
      filter_type = 'ally'
      msg = "Stamm #{which}"
    end
    c = ChannelSetting.first :channel => channel
    if c
      TickerSetting.create({:channel_setting_id => c.id, :server => server, :filter_type => filter_type, :filter => which})
      m.reply "Ok mach ich"
      @bot.action channel, "tickert hier nun für den #{msg}"
      load_ticker_settings
    else
      m.reply "Konnte den Channel nicht finden :("
    end
  end
  
  def handle_deactivate_ticker(m, params)
    channel = params[:channel].to_s
    which   = params[:which].to_s
    
    c = ChannelSetting.first(:channel => channel)
    if which == 'alle'
      TickerSetting.all(:channel_setting_id => c.id).destroy!
    else
      TickerSetting.first(:filter => which).destroy
    end
    
    m.reply "Okay mach ich"
    @bot.action channel, "hat #{which} adelticker für diesen channel gelöscht"
    load_ticker_settings
  end
  
  def handle_ds_stats(m, params)
    return if spam_protect(m)
    server = get_server(m, params)
    case params[:what].downcase
    when "spieler"
      count = Player.count(:server => server).to_s
      m.reply "Es gibt #{Bold}#{number_with_delimiter(count)}#{Bold} Spieler auf Server #{server}"
    when "stämme"
      count = Ally.count(:server => server).to_s
      m.reply "Es gibt #{Bold}#{number_with_delimiter(count)}#{Bold} Stämme auf Server #{server}"
    when "dörfer"
      count = Village.count(:server => server).to_s
      m.reply "Es gibt #{Bold}#{number_with_delimiter(count)}#{Bold} Dörfer auf Server #{server}"
    when "barbarendörfer", "graue"
      count = Village.count(:server => server, :player_dsid => 0).to_s
      m.reply "Es gibt #{Bold}#{number_with_delimiter(count)}#{Bold} Barbarendörfer auf Server #{server}"
    when "adelungen"
      count = Conquer.count(:server => server).to_s
      m.reply "Es gibt #{Bold}#{number_with_delimiter(count)}#{Bold} Adelungen auf Server #{server}"
    when "grauadelungen"
      count = Conquer.count(:server => server, :old_player_dsid => 0).to_s
      m.reply "Es gibt #{Bold}#{number_with_delimiter(count)}#{Bold} Grauadelungen auf Server #{server}"
    when "noobs", "trottel"
      m.reply "Sorry, aber soweit kann ich nicht zählen :("
    else
      m.reply "Weiß ich auch nicht :/"
    end
  end
  
  def handle_ds_whois_player(m, params)
    return if spam_protect(m)
    who = params[:player].to_s
    server = get_server(m, params)
    
    player = get_player(who, server, m)
    
    if player.is_a?(Player)
      ally_s = player.ally.nil? ? '' : "ist im Stamm #{player.ally.tag} (#{player.ally.name}), "
      m.reply "#{Bold}#{player.name}#{Bold} #{ally_s}hat #{number_with_delimiter(player.villages)} Dörfer, #{number_with_delimiter(player.points)} Punkte und ist auf Rang #{number_with_delimiter(player.rank)}."
      m.reply "* #{player.bash_type} hat #{number_with_delimiter(player.bash_off_points)} Offpunkte (Rang #{number_with_delimiter(player.bash_off_rank)}), #{number_with_delimiter(player.bash_def_points)} Defpunkte (Rang #{number_with_delimiter(player.bash_def_rank)}) und #{number_with_delimiter(player.bash_all_points)} Punkte (Rang #{number_with_delimiter(player.bash_all_rank)}) insgesamt."
      m.reply "* #{format_conquers(player)}"
    elsif player.is_a?(Array) && player.size > 0
      playerlist_s = ''
      player.each { |p| playerlist_s += "#{p.name}, " } 
      playerlist_s = playerlist_s[0..-3]
      m.reply "Konnte keinen Spieler mit diesem Namen finden, aber ähnliche: #{playerlist_s}" 
    else
      m.reply "Konnte keinen Spieler mit diesem Namen finden :("
    end
  end
  
  def handle_ds_whois_player_pro(m, params)
    return if spam_protect(m)
    who = params[:player].to_s
    server = get_server(m, params)
    
    player = get_player(who, server, m)
    
    if player.is_a?(Player)
      points_per_village = player.points / player.villages
      
    else
      m.reply "Konnte keinen Spieler mit diesem Namen finden :("
    end
  end
  
  def handle_ds_whois_player_assign(m, params)
    return if spam_protect(m)
    name = params[:name].to_s
    player_name = params[:player].to_s
    server = get_server(m, params)
    player = Player.first(:name => player_name, :server => server)
    if player
      WhoisPlayerInfo.create({:name => name, :player_dsid => player.ds_id, :server => server, :created_at => Time.now})
      server_info = (server.nil? || server.empty?) ? '' : " auf Server #{server}"
      m.reply "Danke, gut zu wissen das #{name} den Account #{player_name}#{server_info} spielt :)"
    else
      m.reply "Den Account #{player_name} gibt's gar nicht :/"
    end
  end
  
  def handle_ds_whois_ally(m, params)
    return if spam_protect(m)
    which = params[:tag].to_s
    server = get_server(m, params)
    ally = get_ally(which, server)
    if ally
      points_average = ((ally.all_points * 1.0) / ally.members).round
      m.reply "#{Bold}#{ally.tag}#{Bold} (#{ally.name}) hat #{ally.members} Member, #{number_with_delimiter(ally.villages)} Dörfer, #{number_with_delimiter(ally.all_points)} Punkte (#{number_with_delimiter(points_average)}/Member) und ist auf Rang #{number_with_delimiter(ally.rank)}"
      m.reply "* #{self.format_conquers(ally)}"
    else
      m.reply "Konnte keinen Stamm mit diesem Tag finden :("
    end
  end
  
  def handle_ds_whois_village(m, params)
    x, y = params[:coordinates].to_s.split('|')
    reply_village_info(x, y, m, params)
  end
  
  def unreplied(m)
    return unless PrivMessage === m
    return if !@channel_settings[m.target.to_s] || !(@channel_settings[m.target.to_s].auto_village >= 1)
    match = /[0-9]{3}\|[0-9]{3}/.match(m.plainmessage)
    if match
      x, y = match[0].split "|"
      reply_village_info(x, y, m, {}, @channel_settings[m.target.to_s].auto_village)
    end
  end
  
  # level 1: nur Link
  # level 2: nur Info
  # level 3: Info + Link
  def reply_village_info(x, y, m, params = {}, level = 3)
    return if spam_protect(m)
    server = get_server(m, params)
    village = Village.first(:x => x.to_i, :y => y.to_i, :server => server)
    if village
      link_s = "http://#{village.server}.die-staemme.de/page.php?page=inbound&screen=info_village&id=#{village.ds_id}"
      if level >= 2
        player_info = format_player(village.player, :default => 'keinem')
        village_info = "#{format_village(village)} gehört #{player_info}"
        village_info +=  " (#{link_s})" if level == 3
        m.reply village_info
      elsif level == 1
        m.reply "#{village.x}|#{village.y}: #{link_s}"
      end
    elsif level >= 2
      m.reply "Konnte kein Dorf mit den Koordinaten #{x.to_s}|#{y.to_s} finden :("
    end
  end
  
  def handle_ds_village_conquer(m, params)
    return if spam_protect(m)
    server = get_server(m, params)
    x, y = params[:coordinates].to_s.split('|')
    if village = Village.first(:x => x, :y => y, :server => server)
      conquers = Conquer.all(:village_dsid => village.ds_id, :server => server, :order => [:conquered_at.desc], :limit => 2)
      if conquers.size > 0
        conquers.each do |c|
          m.reply format_conquer(c, :include_date => true)
        end
      else
        m.reply "#{format_village(village)} wurde nie geadelt"
      end
    else
      m.reply "Konnte kein Dorf mit den Koordinaten #{x.to_s}|#{y.to_s} finden :("
    end
  end
  
  def handle_ds_conquer_info(m, params)
    return if spam_protect(m)
    which  = params[:which].to_s
    days   = params[:days].to_i
    server = get_server(m, params)
    
    since_date = Date.today - (days-1)
    days_s = (days == 1) ? 'heute' : "in den letzten #{days} Tagen"
    
    ally = get_ally(which, server)
    if ally.is_a?(Ally)
      m.reply "#{Bold}#{ally.tag}#{Bold} (#{ally.name}) hat #{days_s} #{format_conquers(ally, since_date)}"
    end
    
    player = get_player(which, server, m)
    if player.is_a?(Player)
      m.reply "#{Bold}#{player.name}#{Bold} hat #{days_s} #{format_conquers(player, since_date)}"
    end
    
    if ally.nil? && player.nil?
      m.reply "Konnte nichts finden was auf #{which} passt :("
    end
  end
  
  def handle_ds_conquer_list(m, params)
    return if spam_protect(m)
    which = params[:which].to_s
    what  = params[:what].to_s
    server = get_server(m, params)
    
    player = get_player(which, server, m)
    if player.is_a?(Player)
      conquers = what == 'verloren' ? player.lost_conquers(3) : player.won_conquers(3)
      conquers.each do |c|
        m.reply format_conquer(c, :include_date => true)
      end
    end
    
    ally = get_ally(which, server)
    if ally.is_a?(Ally)
      conquers = what == 'verloren' ? ally.lost_conquers(3) : ally.won_conquers(3)
      conquers.each do |c|
        m.reply format_conquer(c, :include_date => true)
      end
    end
    
    if ally.nil? && player.nil?
      m.reply "Konnte nichts finden was auf #{which} passt :("
    end
  end
  
  def handle_dschannelinfo(m, params)
    return if spam_protect(m)
    chs = @channel_settings[m.target.to_s]
    what = params[:what].to_s
    if chs
      case what
      when 'ticker'
        tickers = TickerSetting.all(:channel_setting_id => chs.id)
        ret_a = []
        tickers.each do |ticker|
          ret = "##{ticker.id}"
          ret += " Stamm #{Bold}#{ticker.ally}#{Bold}" unless ticker.ally.blank?
          ret += ' auf' if !ticker.ally.blank? && !ticker.continent.blank?
          ret += " Kontinent #{Bold}#{ticker.continent}#{Bold}" unless ticker.continent.blank?
          ret_a << ret
        end
        m.reply 'Aktive Ticker: ' + ret_a.join(', ')
      else
        ticker_count = TickerSetting.count(:channel_setting_id => chs.id)
        m.reply "Settings für den channel #{chs.channel}: Default server #{chs.server}, Auto-Dorfinfo #{chs.auto_village_name}, Ticker aktiv: #{ticker_count}"
      end
    else
      m.reply "Dieser channel ist nicht konfiguriert."
    end
  end
  
  #########
  # Admin #
  #########
  
  def handle_dsmaintance(m, param)
    case param[:action].to_s.downcase
    when 'ticker_status'
      m.reply "Ticker status: #{TickerSetting.all.inspect}"
    when 'm_inspect'
      m.reply "m inspect: #{m.inspect}"
    when 'm_target'
      m.reply "target: #{m.target.to_s}"
    when 'channel_inspect'
      m.reply "channel inspect: #{@channel_settings.inspect}"
    when 'db_info'
      village_c = number_with_delimiter(Village.count)
      player_c = number_with_delimiter(Player.count)
      ally_c = number_with_delimiter(Ally.count)
      conquer_c = number_with_delimiter(Conquer.count)
      m.reply "In der Datenbank sind #{village_c} Dörfer, #{player_c} Spieler, #{ally_c} Stämme und #{conquer_c} Adelungen"
    when 'reload_channel_settings'
      load_channel_settings
      m.reply "ChannelSettings für bot_id #{@bot_id} wurden neu geladen (#{@channel_settings.size} Channel konfiguriert)"
    when 'reload_ticker_settings'
      load_ticker_settings
      m.reply "TickerSettings für bot_id #{@bot_id} wurden neu geladen (#{@ticker_settings.size} Ticker konfiguriert)"
    when 'list_channel_settings'
      m.reply "Channels für bot_id #{@bot_id}: " + @channel_settings.map{|cs| cs.channel}.join(', ')
    when 'gc'
      GC.start
      send_status_msg "GC gestartet"
    when 'stop_ticker'
      stop_ticker_timer
      m.reply "Ticker-Timer ist nun gestoppt"
    when 'start_ticker'
      start_ticker_timer
      m.reply "Ticker-Timer ist nun gestartet"
    when 'color_test'
      #m.reply "#{Color}#{ColorCode[1]}1#{Color} #{Color}#{ColorCode[2]}2#{Color} #{Color}#{ColorCode[3]}3#{Color} #{Color}#{ColorCode[4]}4#{Color} #{Color}#{ColorCode[5]}5#{Color} #{Color}#{ColorCode[6]}6#{Color} #{Color}#{ColorCode[7]}7#{Color} #{Color}#{ColorCode[8]}8#{Color} #{Color}#{ColorCode[9]}9#{Color} #{Color}#{ColorCode[10]}10#{Color} #{Color}#{ColorCode[11]}11#{Color} #{Color}#{ColorCode[11]}11#{Color} #{Color}#{ColorCode[12]}12#{Color} #{Color}#{ColorCode[13]}13#{Color} #{Color}#{ColorCode[14]}14#{Color} #{Color}#{ColorCode[15]}15#{Color} #{Color}#{ColorCode[16]}16#{Color}"
      m.reply "#{Color}1 1#{Color} #{Color}2 2#{Color} #{Color}3 3#{Color} #{Color}4 4#{Color} #{Color}5 5#{Color} #{Color}6 6#{Color} #{Color}7 7#{Color} #{Color}8 8#{Color} #{Color}9 9#{Color} #{Color}10 10#{Color} #{Color}11 11#{Color} #{Color}12 12#{Color} #{Color}13 13#{Color} #{Color}14 14#{Color} #{Color}15 15#{Color} #{Color}16 16#{Color}"
    else
      m.reply "Was soll ich machen?"
    end
  end
  
  def handle_dsdbinfo(m, param)
    server = get_server(m, param)
    if server.blank?
      village_c = number_with_delimiter(Village.count)
      player_c = number_with_delimiter(Player.count)
      ally_c = number_with_delimiter(Ally.count)
      conquer_c = number_with_delimiter(Conquer.count)
      m.reply "In der Datenbank sind #{village_c} Dörfer, #{player_c} Spieler, #{ally_c} Stämme und #{conquer_c} Adelungen"
    else
      village_c = number_with_delimiter(Village.count(:server => server))
      player_c = number_with_delimiter(Player.count(:server => server))
      ally_c = number_with_delimiter(Ally.count(:server => server))
      conquer_c = number_with_delimiter(Conquer.count(:server => server))
      m.reply "In der Datenbank sind #{village_c} Dörfer, #{player_c} Spieler, #{ally_c} Stämme und #{conquer_c} Adelungen von Server #{server}"
    end
  end
  
  def handle_dsimport(m, params)
    what = params[:what].to_s
    serverp = params[:server].to_s
    debug = params[:debug].to_s == 'debug' ? [:debug, :error] : [:error]
    
    if serverp == 'alle'
      servers = @bot.config['ds.import_servers']
    else
      servers = [serverp]
    end
    
    import_data(servers, what, debug)
  end
  
  def handle_announcement(m, params)
    text = params[:text].to_s
    action = params[:action].to_s == 'action' ? true : false
    send_announcement(text, action)
  end
  
  def send_announcement(text, action = false)
    @channel_settings.each do |channel, chsetting|
      if action
        @bot.action channel, text
      else
        @bot.say channel, text
      end
    end
  end
  
  def handle_dsaddchannel(m, params)
    channel       = params[:channelname].to_s
    password      = params[:password].to_s
    server        = params[:server].to_s
    village_info  = params[:village_info].to_i
    owner         = params[:owner].to_s
    channel = '#' + channel unless channel[0..0] == '#'
    ChannelSetting.create({:bot_id => @bot_id, :channel => channel, :channel_pw => password, :server => server, :auto_village => village_info, :owner => owner, :created_at => Time.now})
    m.reply "Channel #{channel} mit dem default Server #{server}, Auto-Dorfinfo #{village_info.to_s} und owner #{owner} wurde mir (bot_id: #{@bot_id}) hinzugefügt"
    load_channel_settings
  end
  
  def handle_dschangechannel(m, params)
    channel = params[:channel].to_s
    what    = params[:what].to_s
    to      = params[:to].to_s
    to      = to.to_i if what == 'auto_village'
    if c = ChannelSetting.first(:channel => channel)
      if c.update_attributes(what.to_sym => to)
        m.reply "Okay, #{what} wurde für den Channel #{channel} auf #{to.to_s} gesetzt."
        load_channel_settings
      else
        m.reply "Sorry, konnte #{what} für den Channel #{channel} nicht auf #{to.to_s} setzen :("
      end
    else
      m.reply "Der Channel ist nicht konfiguriert :/"
    end
  end
  
  def handle_dsremovechannel(m, params)
    channel = params[:channel].to_s
    c = ChannelSetting.first(:channel => channel)
    if c && c.destroy
      m.reply "ChannelSetting für #{channel} wurde gelöscht"
      load_channel_settings
    else
      m.reply "Konnte ChannelSetting für #{channel} nicht löschen :/"
    end
  end
  
  def handle_dsrejoin(m, params)
    channel_p = params[:channel].to_s
    unless channel_p.empty?
      channel = @channel_settings.map {|c,s| c }
    else
      channel = [channel_p]
    end
    channel.each do |c|
      ch = m.server.channel(c)
      unless ch.has_user?(@bot.nick)
        m.reply "Rejoining #{c}..."
        pw = !@channel_settings[c].blank? ? @channel_settings[c].channel_pw : ''
        join_channel(c, pw)
        m.reply ch.has_user?(@bot.nick) ? '...done' : '...failed'
      end
    end
  end
  
  # private
  
  def get_server(m, params)
    params_server = params[:server].to_s
    if (params_server.nil? || params_server.empty?)
      chs = @channel_settings[m.target.to_s]
      unless chs.nil?
        server = chs.server
      else
        m.reply "Dieser Channel ist nicht konfiguriert, bitte Server mit angeben (... auf server xx)"
        raise 'Kein Server angegeben und keine ChannelSetting gefunden'
      end
    else
      server = params_server.downcase
      server = "de#{server}" if server.length < 3
    end
    server
  end
  
  def get_player(who, server, m)
    player = Player.first(:conditions => ["(server = ?) AND (LOWER(name) = LOWER(?))", server, who])
    
    if !player && (whois_info = WhoisPlayerInfo.first(:conditions => ["(LOWER(name) = LOWER(?)) AND (server = ?)", who, server]))
      player = Player.first(:ds_id => whois_info.player_dsid, :server => server)
      m.reply "Es gibt keinen Account mit dem Namen #{who}, der Name wurde aber #{player.name} zugeordnet" if player 
    elsif !player && who.length >= 4
      playerlist = Player.all(:conditions => ["(server = ?) AND (name ILIKE ?)", server, "%#{who}%"], :limit => 15)
      if playerlist.size == 1
        player = playerlist[0]
      else
        player = playerlist
      end
    end
    player
  end
  
  def get_ally(which, server)
    Ally.first(:conditions => ["(server = ?) AND (LOWER(tag) = LOWER(?))", server, which])
  end
  
  def format_conquer(conquer, options = {})
    begin
      formated = format_village(conquer.village)
      if conquer.old_player
        formated += " von #{conquer.old_player.name}"
        formated += " (#{conquer.old_player.ally.tag})" if conquer.old_player.ally
      end
      formated += " wurde geadelt von #{conquer.new_player.name}"
      formated += " (#{conquer.new_player.ally.tag})" if conquer.new_player.ally
      formated += conquer.conquered_at.strftime(" um %H:%M:%S")
      formated += conquer.conquered_at.strftime(" am %d.%m.") if options[:include_date]
    rescue Exception => e
      formated = '-Fehler-'
      send_status_msg "Error while formating conquer ##{conquer.id}: #{e.inspect}"
    end
    formated
  end
  
  def format_conquers(param, since_date = nil)
    "#{number_with_delimiter(param.won_conquers_count(since_date))} Dörfer geadelt, #{number_with_delimiter(param.lost_conquers_count(since_date))} Dörfer verloren (#{number_with_delimiter(param.self_conquers_count(since_date))} mal überadelt) und #{number_with_delimiter(param.nonplayer_conquers_count(since_date))} mal (#{param.nonplayer_conquers_rate(since_date)}%) grau geadelt."
  end
  
  def format_village(village)
    k = get_continent(village.x, village.y)
    "#{village.name} (#{village.x}|#{village.y} K#{k} #{number_with_delimiter(village.points)}P)"
  end
  
  def format_player(player, options = {})
    return options[:default].to_s if player.nil?
    ally_info = player.ally.nil? ? '' : "(#{player.ally.tag})"
    player_info = "#{player.name} #{ally_info}"
  end
  
  def coordinates_on_continent?(x, y, continent)
     get_continent(x, y) == continent.to_i
  end
  
  def get_continent(x, y)
    ((y / 100).floor.to_s + (x / 100).floor.to_s).to_i
  end
  
  def number_with_delimiter(number, delimiter=".", separator=",")
    begin
      parts = number.to_s.split('.')
      parts[0].gsub!(/(\d)(?=(\d\d\d)+(?!\d))/, "\\1#{delimiter}")
      parts.join separator
    rescue
      number
    end
  end
  
  def spam_protect(m)
    time_now = Time.now.to_i
    last_channel_action = @last_actions[:channel][m.target]
    last_user_action    = @last_actions[:user][m.source]
    
    unless last_channel_action.nil?
      if (time_now - last_channel_action) < 2
        return true
      else
        @last_actions[:channel][m.target] = Time.now.to_i
      end
    end
    unless last_user_action.nil?
      if (time_now - last_user_action) < 5
        return true
      else
        @last_actions[:user][m.source]    = Time.now.to_i
      end
    end
    if @channel_settings[m.target.to_s]
      @channel_settings[m.target.to_s].update_attributes :usage_count => @channel_settings[m.target.to_s].usage_count.to_i+1
    end
    return false
  end
  
  def load_channel_settings
    @channel_settings = {}
    ChannelSetting.all(:bot_id => @bot_id).each { |chsetting| @channel_settings[chsetting.channel] = chsetting }
    #send_status_msg "ChannelSettings für bot_id #{@bot_id} wurden neu geladen (#{@channel_settings.size} Channel konfiguriert)"
  end
  
  def load_ticker_settings
    @ticker_settings = TickerSetting.all(:channel_setting_id.in => @channel_setting_ids)
  end
  
  def join_channel(channel, channel_pw = '')
    if channel_pw.nil? || channel_pw == ''
      @bot.join channel
    else
      @bot.join channel, channel_pw
    end
  end
  
  def import_data(servers, what, debug = [:error])
    servers.each do |server|
      send_status_msg "[#{server}]: Importing..."
      w = WorldDataImport.new
      w.init(server, debug, @bot)
      
      sleep(1)
      dur = 0
      if (what == 'normal') || (what == 'alles') || (what == 'dörfer')
        w.import_villages
      end
      if (what == 'normal') || (what == 'alles') || (what == 'min') || (what == 'stämme')
        w.import_allies
      end
      if (what == 'normal') || (what == 'alles') || (what == 'min') || (what == 'spieler')
        w.import_players
      end
      if (what == 'adelungen') || (what == 'alles')
        w.delete_conquers
        sleep(2)
        w.import_conquers
      end
      if (['adelungen_neu', 'neue_adelungen', 'neue_adelungen23'].include?(what))
        stop_conquer_import_timer
        if (what == 'adelungen_neu')
          w.delete_conquers
          sleep(2)
          w.import_conquers
        end
        if (what == 'neue_adelungen')
          w.import_conquers_since(Time.now.to_i - (60*60))
        end
        if (what == 'neue_adelungen23')
          w.import_conquers_since(Time.now.to_i - (23*60*60))
        end
        start_conquer_import_timer
      end
      
      send_status_msg "[#{server}]: Import ##{@import_run.to_i.to_s} finished!"
      w = nil
      GC.start
      sleep(2)
    end
  end
  
  def send_status_msg(msg)
    @bot.say '#dsbot-status', msg
  end

end

plugin = DieStaemmePlugin.new

plugin.map 'wie viele :what gibt es[ auf server :server]?', :action => 'handle_ds_stats', :auth_path => 'info'
plugin.map 'spieler *player [auf server :server]', :action => 'handle_ds_whois_player', :auth_path => 'info'
plugin.map 'wer ist *player [auf server :server]', :action => 'handle_ds_whois_player', :auth_path => 'info'
plugin.map 'stamm *tag [auf server :server]', :action => 'handle_ds_whois_ally', :auth_path => 'info'
plugin.map 'dorf :coordinates [auf server :server]', :action => 'handle_ds_whois_village', :requirements => {:coordinates => /^[0-9]{3}\|[0-9]{3}$/}, :auth_path => 'info'
plugin.map 'dorfadelungen :coordinates [auf server :server]', :action => 'handle_ds_village_conquer', :requirements => {:coordinates => /^[0-9]{3}\|[0-9]{3}$/}, :auth_path => 'info'
plugin.map 'adel *which [in :days tagen] [auf server :server]', :action => 'handle_ds_conquer_info', :defaults => {:days => 1}, :auth_path => 'info'
plugin.map 'adelungen *which [hat :what] [auf server :server]', :action => 'handle_ds_conquer_list', :defaults => {:what => 'gewonnen'}, :auth_path => 'info'
plugin.map 'dschannelinfo [:what]', :action => 'handle_dschannelinfo', :auth_path => 'info', :private => false
plugin.map 'hilfe [:cmd]', :action => 'hilfe', :auth_path => 'info'

plugin.map 'whois-info: *name spielt den account *player [auf server :server]', :action => 'handle_ds_whois_player_assign', :auth_path => 'chedit'
plugin.map 'adelticker in :channel fuer den :what :which [auf server :server]', :action => 'handle_activate_ticker', :auth_path => 'chedit'
plugin.map 'deaktiviere :which adelticker in :channel', :action => 'handle_deactivate_ticker', :auth_path => 'chedit'

plugin.map 'dsmaintance :action [*option]', :action => 'handle_dsmaintance', :threaded => true
plugin.map 'dsdbinfo [:server]', :action => 'handle_dsdbinfo', :threaded => true
plugin.map 'dsimport :what von server :server [:debug]', :action => 'handle_dsimport', :threaded => true
plugin.map 'announcement :action *text', :action => 'handle_announcement'
plugin.map 'dsaddchannel :channelname [pw :password] [dorfinfo :village_info] :server [owner :owner]', :action => 'handle_dsaddchannel'
plugin.map 'dschangechannel :channel :what :to', :action => 'handle_dschangechannel'
plugin.map 'dsremovechannel :channel', :action => 'handle_dsremovechannel'
plugin.map 'dsrejoin [*channel]', :action => 'handle_dsrejoin'

plugin.default_auth('*', false)
plugin.default_auth('chedit', false)
plugin.default_auth('info', true)
