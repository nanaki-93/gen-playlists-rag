package org.github.nanaki_93.gen_playlists_rag.data


import ch.systemsx.cisd.hdf5.HDF5Factory
import ch.systemsx.cisd.hdf5.IHDF5SimpleReader
import com.opencsv.CSVWriter
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths



fun main() {
    val input = "D:\\DEV\\lab\\gen-playlists-rag\\src\\main\\resources\\data\\MillionSongSubset"
    val output = "D:\\DEV\\lab\\gen-playlists-rag\\src\\main\\resources\\data\\songs-v3.csv"


    // Initialize parser
    val dataParser = DataParser()

    try {
        val songsData = run {
            // Parse directory
            println("Parsing directory: $input")
            dataParser.parseDirectory(input, null)
        }

        if (songsData.isEmpty()) {
            println("Error: No data was parsed successfully")
            kotlin.system.exitProcess(1)
        }

        // Save to CSV
        dataParser.saveToCsv(songsData, output)

        val stats = dataParser.getDatasetStats(songsData)
        printStats(stats)


        println("\n✅ Successfully converted ${songsData.size} songs to $output")

    } catch (e: Exception) {
        when (e) {
            is InterruptedException -> {
                println("\n⌐ Process interrupted by user")
                kotlin.system.exitProcess(1)
            }

            else -> {
                println("Error: Unexpected error: ${e.message}")
                kotlin.system.exitProcess(1)
            }
        }
    }
}

data class SongData(
    val data: MutableMap<String, String?> = mutableMapOf()
) {
    operator fun get(key: String): Any? = data[key]
    operator fun set(key: String, value: Any?) {
        data[key] = processValue(value)
    }

    fun keys(): Set<String> = data.keys
}

private fun processValue(value: Any?): String? {
    return when (value) {
        is ByteArray -> String(value, Charsets.UTF_8).trim().ifEmpty { null }
        is String -> value.trim().ifEmpty { null }
        is Float -> if (value.isNaN()) null else value.toString()
        is Double -> if (value.isNaN()) null else value.toString()
        else -> value.toString()
    }
}

data class DatasetStats(
    val totalSongs: Int = 0,
    val uniqueArtists: Int = 0,
    val yearRange: Pair<Int, Int>? = null,
    val avgDuration: Double? = null,
    val missingDataPercentage: Map<String, Double> = emptyMap(),
    val genreCoverage: Map<String, Any> = emptyMap()
)

class DataParser {

    companion object {
        private const val LOG_TAG = "MSDParser"
    }

    // Define the fields we want to extract from MSD compound datasets
    private val metadataFields = mapOf(
        "song_id" to "song_id",
        "title" to "title",
        "artist_name" to "artist_name",
        "artist_id" to "artist_id",
        "album_name" to "release",
        "genre" to "genre",
        "artist_hotttnesss" to "artist_hotttnesss",
        "artist_familiarity" to "artist_familiarity",
        "artist_location" to "artist_location",
        "artist_latitude" to "artist_latitude",
        "artist_longitude" to "artist_longitude",
        "song_hotttnesss" to "song_hotttnesss",
        "track_7digitalid" to "track_7digitalid",
        "artist_7digitalid" to "artist_7digitalid",
        "release_7digitalid" to "release_7digitalid",
        "artist_mbid" to "artist_mbid",
        "artist_playmeid" to "artist_playmeid"
    )

    private val analysisFields = mapOf(
        "track_id" to "track_id",
        "duration" to "duration",
        "tempo" to "tempo",
        "key" to "key",
        "key_confidence" to "key_confidence",
        "mode" to "mode",
        "mode_confidence" to "mode_confidence",
        "time_signature" to "time_signature",
        "time_signature_confidence" to "time_signature_confidence",
        "danceability" to "danceability",
        "energy" to "energy",
        "loudness" to "loudness",
        "end_of_fade_in" to "end_of_fade_in",
        "start_of_fade_out" to "start_of_fade_out",
        "audio_md5" to "audio_md5",
        "analysis_sample_rate" to "analysis_sample_rate"
    )

    private val musicbrainzFields = mapOf(
        "year" to "year"
    )

    private val computedFields = listOf(
        "segments_count",
        "sections_count",
        "beats_count",
        "bars_count",
        "tatums_count",
        "segments_avg_duration",
        "segments_avg_loudness_max",
        "segments_avg_confidence"
    )

    fun extractSongData(reader: IHDF5SimpleReader): SongData {
        val songData = SongData()

        // Extract metadata from compound dataset
        extractPath(reader, "/metadata/songs", songData, metadataFields)
        // Extract analysis from compound dataset
        extractPath(reader, "/analysis/songs", songData, analysisFields)
        // Extract MusicBrainz data from compound dataset
        extractPath(reader, "/musicbrainz/songs", songData, musicbrainzFields)

        // Extract similar artists array
        extractStringArrayPath(reader, "/metadata/similar_artists", "similar_artists", songData)
        // Extract artist terms
        extractStringArrayPath(reader, "/metadata/artist_terms", "artist_terms", songData)

        // Extract computed features from analysis data
        try {
            val analysisFeatures = extractAnalysisFeatures(reader)
            analysisFeatures.forEach { (key, value) ->
                songData[key] = value
            }
        } catch (e: Exception) {
            println("Debug: Could not extract analysis features: ${e.message}")
            computedFields.forEach { songData[it] = null }
        }

        return songData
    }


    private fun extractPath(
        reader: IHDF5SimpleReader,
        pathToExtract: String,
        songData: SongData,
        mapToInspect: Map<String, Any?>
    ) {
        try {
            if (reader.exists(pathToExtract)) {
                val metadataCompound = reader.readCompoundArray(pathToExtract, Map::class.java)
                if (metadataCompound.isNotEmpty()) {
                    val metadataRecord = metadataCompound[0] as Map<*, *>

                    mapToInspect.forEach { (csvField, h5Field) ->
                        try {
                            val value = metadataRecord[h5Field]
                            songData[csvField] = value
                        } catch (e: Exception) {
                            println("Debug: Could not extract metadata field $h5Field: ${e.message}")
                            songData[csvField] = null
                        }
                    }
                }
            } else {
                mapToInspect.keys.forEach { songData[it] = null }
            }
        } catch (e: Exception) {
            println("Debug: No $pathToExtract found")
            mapToInspect.keys.forEach { songData[it] = null }
        }
    }

    private fun extractStringArrayPath(
        reader: IHDF5SimpleReader,
        path: String,
        fieldName: String,
        songData: SongData,
        maxItems: Int = 5,
        joinSeparator: String = ","
    ) {
        try {
            if (reader.exists(path)) {
                val stringArray = reader.readStringArray(path)
                if (stringArray.isNotEmpty()) {
                    val filteredList = stringArray.take(maxItems).filter { it.isNotBlank() }
                    songData[fieldName] = if (filteredList.isNotEmpty()) {
                        filteredList.joinToString(joinSeparator)
                    } else null
                } else {
                    songData[fieldName] = null
                }
            } else {
                songData[fieldName] = null
            }
        } catch (e: Exception) {
            println("Debug: No $fieldName found")
            songData[fieldName] = null
        }
    }


    private fun extractAnalysisFeatures(reader: IHDF5SimpleReader): Map<String, Any?> {
        val features = mutableMapOf<String, Any?>()

        // Extract segments data
        try {
            val segmentsStart = reader.readFloatArray("/analysis/segments_start")
            val segmentsConfidence = reader.readFloatArray("/analysis/segments_confidence")
            val segmentsLoudnessMax = reader.readFloatArray("/analysis/segments_loudness_max")

            features["segments_count"] = segmentsStart.size
            features["segments_avg_confidence"] = if (segmentsConfidence.isNotEmpty()) {
                segmentsConfidence.average()
            } else null
            features["segments_avg_loudness_max"] = if (segmentsLoudnessMax.isNotEmpty()) {
                segmentsLoudnessMax.average()
            } else null

            // Calculate average segment duration
            if (segmentsStart.size > 1) {
                val durations = (1 until segmentsStart.size).map { i ->
                    segmentsStart[i] - segmentsStart[i - 1]
                }
                features["segments_avg_duration"] = durations.average()
            } else {
                features["segments_avg_duration"] = null
            }

        } catch (e: Exception) {
            listOf("segments_count", "segments_avg_confidence", "segments_avg_loudness_max", "segments_avg_duration")
                .forEach { features[it] = null }
        }

        // Extract other analysis counts
        val analysisArrays = mapOf(
            "sections_start" to "sections_count",
            "beats_start" to "beats_count",
            "bars_start" to "bars_count",
            "tatums_start" to "tatums_count"
        )

        analysisArrays.forEach { (arrayName, countField) ->
            try {
                if (reader.exists("/analysis/$arrayName")) {
                    val data = reader.readFloatArray("/analysis/$arrayName")
                    features[countField] = data.size
                } else {
                    features[countField] = null
                }
            } catch (e: Exception) {
                features[countField] = null
            }
        }

        return features
    }

    fun parseSingleFile(filePath: String): SongData? {
        return try {
            val reader = HDF5Factory.openForReading(filePath)
            val result = extractSongData(reader)
            reader.close()
            result
        } catch (e: Exception) {
            println("Error parsing $filePath: ${e.message}")
            null
        }
    }

    fun parseDirectory(directoryPath: String, maxFiles: Int? = null): List<SongData> {
        val songsData = mutableListOf<SongData>()

        val h5Files = Files.walk(Paths.get(directoryPath))
            .filter { it.toString().endsWith(".h5") }
            .limit(maxFiles?.toLong() ?: Long.MAX_VALUE)
            .toList()

        println("Found ${h5Files.size} HDF5 files to process")

        h5Files.forEachIndexed { index, filePath ->
            val songData = parseSingleFile(filePath.toString())
            if (songData != null) {
                songsData.add(songData)
            }

            if ((index + 1) % 100 == 0) {
                println("Processed ${index + 1} files...")
            }
        }

        println("Successfully parsed ${songsData.size} files")
        return songsData
    }

    fun saveToCsv(songsData: List<SongData>, outputFile: String) {
        if (songsData.isEmpty()) {
            println("Error: No data to save")
            return
        }

        // Get all unique fields from all songs
        val allFields = mutableSetOf<String>()
        songsData.forEach { song ->
            allFields.addAll(song.keys())
        }

        // Define a logical order for the fields
        val priorityFields = listOf(
            "song_id", "track_id", "title", "artist_name", "artist_id", "album_name",
            "year", "duration", "genre",
            // Audio features
            "tempo", "key", "key_confidence", "mode", "mode_confidence",
            "time_signature", "time_signature_confidence",
            "danceability", "energy", "loudness",
            "end_of_fade_in", "start_of_fade_out",
            // Metadata
            "artist_hotttnesss", "artist_familiarity", "song_hotttnesss",
            "artist_location", "artist_latitude", "artist_longitude",
            // Analysis counts
            "segments_count", "sections_count", "beats_count", "bars_count", "tatums_count",
            "segments_avg_duration", "segments_avg_loudness_max", "segments_avg_confidence",
            // Arrays
            "similar_artists", "artist_terms",
            // Technical
            "audio_md5", "analysis_sample_rate",
            "track_7digitalid", "artist_7digitalid", "release_7digitalid",
            "artist_mbid", "artist_playmeid"
        )

        // Create final field order
        val fieldOrder = mutableListOf<String>()
        priorityFields.forEach { field ->
            if (field in allFields) {
                fieldOrder.add(field)
            }
        }

        // Add any remaining fields
        val remainingFields = (allFields - fieldOrder.toSet()).sorted()
        fieldOrder.addAll(remainingFields)

        println("Saving ${songsData.size} songs to $outputFile")
        println("Fields to export: ${fieldOrder.size} columns")

        FileWriter(outputFile).use { fileWriter ->
            val csvWriter = CSVWriter(fileWriter)

            // Write header
            csvWriter.writeNext(fieldOrder.toTypedArray())

            // Write data
            songsData.forEachIndexed { index, song ->
                val row = fieldOrder.map { field ->
                    song[field]?.toString() ?: ""
                }.toTypedArray()

                csvWriter.writeNext(row)

                if ((index + 1) % 1000 == 0) {
                    println("Written ${index + 1} rows...")
                }
            }
        }

        println("CSV file saved successfully: $outputFile")

        // Print sample of first song
        println("Sample data from first song:")
        val firstSong = songsData[0]
        fieldOrder.take(10).forEach { field ->
            var value = firstSong[field]?.toString() ?: "null"
            if (value.length > 50) {
                value = value.take(47) + "..."
            }
            println("  $field: $value")
        }
        if (fieldOrder.size > 10) {
            println("  ... and ${fieldOrder.size - 10} more fields")
        }
    }

    fun getDatasetStats(songsData: List<SongData>): DatasetStats {
        if (songsData.isEmpty()) return DatasetStats()

        val totalSongs = songsData.size
        val uniqueArtists = songsData.mapNotNull { it["artist_name"]?.toString() }.toSet().size

        // Year range
        val years = songsData.mapNotNull {
            val year = it["year"]
            when {
                year is Number && year.toInt() > 0 -> year.toInt()
                year is String && year.toIntOrNull() != null && year.toInt() > 0 -> year.toInt()
                else -> null
            }
        }
        val yearRange = if (years.isNotEmpty()) {
            years.minOrNull()!! to years.maxOrNull()!!
        } else null

        // Average duration
        val durations = songsData.mapNotNull {
            val duration = it["duration"]
            when {
                duration is Number && duration.toDouble() > 0 -> duration.toDouble()
                duration is String && duration.toDoubleOrNull() != null && duration.toDouble() > 0 -> duration.toDouble()
                else -> null
            }
        }
        val avgDuration = if (durations.isNotEmpty()) durations.average() else null

        // Missing data analysis
        val keyFields = listOf("title", "artist_name", "year", "duration", "tempo", "energy", "genre", "artist_terms")
        val missingDataPercentage = keyFields.associateWith { field ->
            val missing = songsData.count { song ->
                val value = song[field]
                value == null || value.toString().isBlank()
            }
            (missing.toDouble() / totalSongs) * 100
        }

        // Genre analysis
        val songsWithGenre = songsData.filter { song ->
            val genre = song["genre"]
            genre != null && genre.toString().isNotBlank()
        }

        val genreCounts = mutableMapOf<String, Int>()
        songsWithGenre.forEach { song ->
            val genres = song["genre"]?.toString()?.split(",") ?: emptyList()
            genres.forEach { genre ->
                val cleanGenre = genre.trim()
                genreCounts[cleanGenre] = genreCounts.getOrDefault(cleanGenre, 0) + 1
            }
        }

        val topGenres = genreCounts.toList().sortedByDescending { it.second }.take(10)

        val genreCoverage = mapOf(
            "songs_with_genre" to songsWithGenre.size,
            "genre_percentage" to (songsWithGenre.size.toDouble() / totalSongs) * 100,
            "top_genres" to topGenres
        )

        return DatasetStats(
            totalSongs = totalSongs,
            uniqueArtists = uniqueArtists,
            yearRange = yearRange,
            avgDuration = avgDuration,
            missingDataPercentage = missingDataPercentage,
            genreCoverage = genreCoverage
        )
    }
}

fun printStats(stats: DatasetStats) {
    println("\n=== Dataset Statistics ===")
    println("Total songs: ${stats.totalSongs}")
    println("Unique artists: ${stats.uniqueArtists}")

    stats.yearRange?.let { (min, max) ->
        println("Year range: $min - $max")
    }

    stats.avgDuration?.let { duration ->
        println("Average duration: ${"%.2f".format(duration)} seconds")
    }

    if (stats.missingDataPercentage.isNotEmpty()) {
        println("\nMissing data percentages:")
        stats.missingDataPercentage.forEach { (field, percentage) ->
            println("  $field: ${"%.1f".format(percentage)}%")
        }
    }

    // Genre coverage stats
    val genreStats = stats.genreCoverage
    if (genreStats.isNotEmpty()) {
        println("\nGenre coverage:")
        val songsWithGenre = genreStats["songs_with_genre"] as? Int ?: 0
        val genrePercentage = genreStats["genre_percentage"] as? Double ?: 0.0
        println("  Songs with genre info: $songsWithGenre (${"%.1f".format(genrePercentage)}%)")

        @Suppress("UNCHECKED_CAST")
        val topGenres = genreStats["top_genres"] as? List<Pair<String, Int>>
        topGenres?.let { genres ->
            if (genres.isNotEmpty()) {
                println("  Top genres:")
                genres.take(5).forEach { (genre, count) ->
                    println("    $genre: $count songs")
                }
                if (genres.size > 5) {
                    println("    ... and ${genres.size - 5} more genres")
                }
            }
        }
    }
}

fun inspectH5Structure(filePath: String) {
    try {
        val reader = HDF5Factory.openForReading(filePath)
        println("\n=== Structure of $filePath ===")

        // Note: This is a simplified structure inspection
        // The full HDF5 structure inspection would require more complex traversal
        println("Available groups and datasets:")

        val commonPaths = listOf(
            "/metadata/songs",
            "/analysis/songs",
            "/musicbrainz/songs",
            "/metadata/similar_artists",
            "/metadata/artist_terms",
            "/metadata/artist_terms_weight",
            "/analysis/segments_start",
            "/analysis/segments_confidence",
            "/analysis/segments_loudness_max",
            "/analysis/sections_start",
            "/analysis/beats_start",
            "/analysis/bars_start",
            "/analysis/tatums_start"
        )

        commonPaths.forEach { path ->
            if (reader.exists(path)) {
                try {
                    val info = reader.getDataSetInformation(path)
                    println("  $path: ${info.dimensions.contentToString()} ${info.typeInformation}")
                } catch (e: Exception) {
                    println("  $path: exists")
                }
            }
        }

        reader.close()
    } catch (e: Exception) {
        println("Error inspecting $filePath: ${e.message}")
    }
}
