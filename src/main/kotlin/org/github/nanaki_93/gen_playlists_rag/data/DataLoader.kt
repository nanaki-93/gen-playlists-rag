package org.github.nanaki_93.gen_playlists_rag.data


import org.github.nanaki_93.gen_playlists_rag.service.QdrantService
import org.springframework.ai.document.Document
import org.springframework.boot.CommandLineRunner
import org.springframework.core.io.ResourceLoader
import org.springframework.stereotype.Component


/**
 * DataLoader is a component that loads initial data into the Qdrant database on application startup.
 * It implements the CommandLineRunner interface to run its logic right after the Spring Boot application context is loaded.
 */
@Component
class DataLoader(
    private val qdrantService: QdrantService,
    private val resourceLoader: ResourceLoader
) : CommandLineRunner {

    override fun run(vararg args: String?) {
        // Check if the vector store is empty
        if(qdrantService.collectionExists() && qdrantService.getVectorStoreSize() != 0L) {
            println("Vector store already exists and contains ${qdrantService.getVectorStoreSize()} points.")
            return
        }


        println("Starting data loading process...")
        try {
            // Use ResourceLoader to read from classpath
            val resource = resourceLoader.getResource("classpath:data/songs.csv")
            val songs = resource.inputStream.bufferedReader().useLines { lines ->
                lines.drop(1) // Skip header
                    .map { line -> line.split(",") }
                    .map { Song(it[0], it[1], it[2], it[3], it[4], it[5], it[6], it[7], it[8], null) }
                    .toList()
            }

            println("Read ${songs.size} songs from data/songs.csv")

            // Convert songs to Spring AI Documents
            val documents = songs.map { song ->
                Document(
                    buildString {
                        append("${song.title} by ${song.artistName}")
                        if (song.albumName.isNotBlank() && song.albumName != "0") {
                            append(" from the album ${song.albumName}")
                        }
                        if (song.year.isNotBlank() && song.year != "0") {
                            append(" released in ${song.year}")
                        }
                        if (song.genre.isNotBlank()) {
                            append(". Genre: ${song.genre}")
                        }
                        if (song.tempo.isNotBlank()) {
                            append(". Tempo: ${song.tempo} BPM")
                        }
                    }
                    ,
                    mapOf(
                        "song_id" to song.songId,
                        "title" to song.title,
                        "artist_name" to song.artistName,
                        "artist_id" to song.artistId,
                        "album_name" to song.albumName,
                        "year" to song.year,
                        "duration" to song.duration,
                        "genre" to song.genre,
                        "tempo" to song.tempo,
                    )
                )
            }
            println("Converted ${songs.size} songs to Spring AI Documents")


            // Add documents to the vector store
            qdrantService.addDocuments(documents)

            println("Data loading complete. Upserted ${documents.size} songs to the vector store.")

        } catch (e: Exception) {
            println("Failed to load data into Qdrant: ${e.message}")
            e.printStackTrace()
        }
    }
}