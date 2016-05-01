# -- encoding: utf-8 --
require 'inifile'
require 'logger'
require 'rubygems'
require 'sqlite3'
#require 'mini_exiftool_custom'

class AppLogger
	def self.log
		if @logger.nil?	
			@logger = Logger.new "picasa2xmp.log"
			@logger.level = Logger::DEBUG
			@logger.progname = 'Picasa2Xmp'
			@logger.datetime_format = '%Y-%m-%d %H:%M:%S '
		end
		@logger
	end
end
  
class PicasaStars2Xmp
	
	def initialize()
		
		#http://sqlite-ruby.rubyforge.org/sqlite3/faq.html#538670656
		@db = SQLite3::Database.new "picasa2xmp.db"
		@db.execute "CREATE TABLE IF NOT EXISTS `Photos` (
			`Path`	TEXT NOT NULL,
			`Name`	TEXT NOT NULL,
			`Rating`	INTEGER,
			`Is_Picasa_Star`	INTEGER,
			PRIMARY KEY(Path, Name)
			);"
		@db.execute "CREATE TABLE IF NOT EXISTS `Folders` (
			`Path`	TEXT NOT NULL,
			`Processed`	INTEGER,
			PRIMARY KEY(Path)
			);"
		@db.execute "CREATE TABLE IF NOT EXISTS `IniEntries` (
			`Ini`		TEXT NOT NULL,
			`Section`	TEXT NOT NULL,
			`Key`		TEXT NOT NULL,
			`Value`		TEXT NOT NULL
			);"
	end
	
	def get_stars(picasa_home)
		AppLogger.log.info "Get star images for image root folder #{picasa_home}"
		stars_map = get_stars_each find_picasa_inis picasa_home
		stars_map
	end
	
	# Suche alle Picasa.ini Dateien im Verzeichnis und in allen Unterverzeichnissen
	# Schreibe das Ergebnis in die Datenbank
	def find_picasa_inis(picasa_home)
		ini_files = Dir.glob(File.join(picasa_home, '**/.picasa.ini'))
		AppLogger.log.info "Total number of .picasa.ini files: #{ini_files.size}"
		
		sql = "DELETE FROM Folders WHERE Path LIKE \"#{picasa_home}%\";"
		ini_files.each do |ini_file|
			sql += "INSERT INTO Folders (Path, Processed) VALUES (\"#{File.dirname(ini_file)}\", \"False\");"
		end
		@db.execute_batch(sql)
		
		ini_files
	end
	
	
	def get_all_keys(picasa_home)
		#keys_map = Hash.new
		ini_files = find_picasa_inis(picasa_home)
		ini_files.each do |ini_file|
			sql = "DELETE FROM IniEntries WHERE Ini = \"#{ini_file}\";"
			ini_obj = IniFile.load(ini_file, {:encoding => 'UTF-8'})
			ini_obj.each_section do |section|
				ini_obj[section].each do |content|
					sql += "INSERT INTO IniEntries (Ini, Section, Key, Value) VALUES (\"#{ini_file}\", \"#{section}\", \"#{content[0]}\", \"#{content[1]}\");"
					#keys_map[content[0]] = content[1]
				end
			end
			@db.execute_batch(sql)
		end
	end
	
	def get_stars_each(ini_files)
		stars_map = Hash.new
		stars_count = 0
		ini_files.each do |ini_file|
			stars_one = get_stars_one ini_file
			if stars_one.size > 0 then
				stars_count += stars_one.size
				stars_map[File.dirname(ini_file)] = stars_one
			end
		end
		stars_map['stars_count' => stars_count]
		stars_map
	end
	
	def find_and_set_stars
	
		# Ermittle unverarbeitete Pfade
		@db.execute("SELECT Path FROM Folders WHERE Processed = \"False\";") do |row|
			# Erstelle Dateiname
			ini_file = File.join(row[0], ".picasa.ini")
			
			# Suche in Ini-Datei nach Bildern, die mit Stern markiert sind
			stars_pictures = get_stars_one ini_file
			
			# Wenn markierte Bilder gefunden wurden, bei diesen das Rating setzen
			if stars_pictures.size > 0 then
				set_rating_result = set_xmp_rating (Hash[File.dirname(ini_file) => stars_pictures])
			end
			
			# Schreibe Bearbeitungsvermerk für den Ordner in die DB-Tabelle
			@db.execute("UPDATE Folders SET Processed = \"True\" WHERE Path = \"#{File.dirname(ini_file)}\";")
			
		end
	end
	
	def get_stars_one(ini_file)
		# load Picasa ini file
		AppLogger.log.debug "load .picasa.ini: #{ini_file}"
		ini_obj = IniFile.load(ini_file, {:encoding => 'UTF-8'})
		
		# look for db entries for ini file and delete these rows
		db_rows = @db.get_first_value("SELECT COUNT(*) FROM Photos WHERE Path=?", [File.dirname(ini_file)])
		if db_rows > 0
			AppLogger.log.warn "Found #{db_rows} DB entries for image directory #{File.dirname(ini_file)}; deleting these entries..."
			@db.execute("DELETE FROM Photos WHERE Path=?", [File.dirname(ini_file)])
		end
		
		# iterate through ini and create array of star images
		stars = Array.new
		ini_obj.each_section do |section|
			if ini_obj[section]['star'] then
				stars << section
			end
		end
		
		# save star images in db
		if stars.size > 0
			sql = ""
			stars.each do |image|
				sql += "INSERT INTO Photos (Path, Name, Is_Picasa_Star) VALUES (\"#{File.dirname(ini_file)}\", \"#{image}\", 1);\n"
			end
			@db.execute_batch(sql)
		end
		
		AppLogger.log.info "Found #{stars.size} star images for #{ini_file}"
		stars
	end
	
	def set_xmp_rating stars_map
		xmp_star_rating = 5
		xmp_errors = Array.new
		xmp_success = 0
		xmp_notfound = 0
		
		begin
			sql = ""
			stars_map.each do |star_directory|
				AppLogger.log.info "Set XMP rating for folder #{star_directory[0]}"
				star_directory[1].each do |star_file|
					# for each star image in a star directory
					photo_path = File.join(star_directory[0], star_file)
					AppLogger.log.debug "Set star rating for #{photo_path}"
					if !File.file?(photo_path)
						xmp_notfound += 1
						AppLogger.log.warn "Could not find star image #{photo_path}"
						next
					end
					photo = MiniExiftool.new (photo_path)
					photo.ignore_minor_errors = true;
					
					# Prüfe, ob bereits gesetzt
					if xmp_star_rating == photo.rating then
						xmp_success += 1
						AppLogger.log.debug "Rating is already set #{photo_path}"
					else
						# Setze Rating
						log_msg = "Old rating: #{photo.rating}"
						photo.rating = xmp_star_rating
						log_msg += ", set rating: #{photo.rating}"
						exif_result = photo.save
						AppLogger.log.debug "Exif result: #{exif_result}" if !exif_result
						photo = nil
						photo = MiniExiftool.new photo_path
						log_msg += ", new rating #{photo.rating}"
						AppLogger.log.debug log_msg
					
						# Prüfe, ob erfolgreich gesetzt
						if xmp_star_rating != photo.rating then
							xmp_errors << photo_path
							AppLogger.log.error "Could not set star rating for image #{photo_path}"
						else
							xmp_success += 1
						end
					end
					sql += "UPDATE Photos SET Rating = #{photo.rating} WHERE Path = \"#{star_directory[0]}\" AND Name =\"#{star_file}\";\n"
				end
			end
			@db.execute_batch(sql)

		rescue MiniExiftool::Error => e
			AppLogger.log.fatal e.message
		rescue SQLite3::Exception => e 
			AppLogger.log.fatal e.message
		end
		
		AppLogger.log.info "Set XMP star rating finished; pictures successfully modified: #{xmp_success}, not found: #{xmp_notfound}, not modified: #{xmp_errors.size}"
		xmp_result = Hash[:xmp_errors => xmp_errors, :xmp_notfound => xmp_notfound, :xmp_success => xmp_success]
	end
	
	def self.process_path
		photo_path = "P:/bjoern"
		p2x = PicasaStars2Xmp.new
		result = p2x.get_stars photo_path
		xmp_result = p2x.set_xmp_rating result
		AppLogger.log.error xmp_result[:xmp_errors] if xmp_result[:xmp_errors].size > 0
	end
	
	def self.test_keys
		#photo_path = "C:/Users/bjoer/Pictures/2016-02-08 Rosenmontag Köln"
		photo_path = "P:/"
		
		p2x = PicasaStars2Xmp.new
		p2x.get_all_keys photo_path
	end
	
	def self.test_db
		begin
			db = SQLite3::Database.new "picasa2xmp.db"
			db.execute("SELECT * FROM Photos").each do |row|
				puts row.join "\s"
			end
		rescue SQLite3::Exception => e 
			puts "Exception occurred"
			puts e
		ensure
			db.close if db
		end
	end
	
	def self.test_encoding test_path1
		#test_path1 = "P:/bjoern/06 Familie/1989/1989-06 Schulausflug + Ilmmünster"
		test_path2 = "P:/bjoern/06 Familie/1989/1989-06 Schulausflug + Ilmmünster"
		test_path1_encoded = test_path1.encode(Encoding.find("UTF-8"))
		puts "encoding test_path1: #{test_path1.encoding} - #{test_path1}"	
		puts "encoding test_path2: #{test_path2.encoding} - #{test_path2}"	
		puts "encoding test_path1_encoded: #{test_path1_encoded.encoding} - #{test_path1_encoded}"
		Dir.chdir test_path2
		puts Dir.pwd
		Dir.chdir "U:/ruby/picasa"
	end
end