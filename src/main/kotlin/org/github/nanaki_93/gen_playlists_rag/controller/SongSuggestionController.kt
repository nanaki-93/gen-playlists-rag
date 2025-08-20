package org.github.nanaki_93.gen_playlists_rag.controller

import org.github.nanaki_93.gen_playlists_rag.data.Song
import org.github.nanaki_93.gen_playlists_rag.dto.SongSearchRequest
import org.github.nanaki_93.gen_playlists_rag.dto.SongSearchResponse
import org.github.nanaki_93.gen_playlists_rag.dto.SongSuggestionRequest
import org.springframework.ai.vectorstore.qdrant.QdrantVectorStore

import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/songs")
class SongSuggestionController (private val vectorStore: QdrantVectorStore){


    @PostMapping("/getSuggestions")
    fun getSongSuggestion(@RequestBody request: SongSearchRequest) :SongSearchResponse{
        val songSuggestion = SongSuggestionRequest(query = createQuery(request))
        val searchResults = vectorStore.similaritySearch(songSuggestion.query)

        return SongSearchResponse(
            results = searchResults.map { document ->
                Song(
                    songId = document.metadata["song_id"] as String,
                    title = document.metadata["title"] as String,
                    artistName = document.metadata["artist_name"] as String,
                    artistId = document.metadata["artist_id"] as String,
                    genre = document.metadata["genre"] as String,
                    albumName = document.metadata["album_name"] as String,
                    year = document.metadata["year"] as String,
                    duration = document.metadata["duration"] as String,
                    tempo = document.metadata["tempo"] as String,
                    score = document.metadata["distance"] as Float,
                )
            }
        )


    }


    //create the prompt query from the request, based on the features we have.
    private fun createQuery(request: SongSearchRequest): String {
        return """
            Find the first ${request.limit} songs closest to the following criteria (exlude the songs that are already in the list of point 4):
            1- ${request.artists.joinToString(" or ")}
            2- ${request.genres.joinToString(" or ")}
            3- ${request.years.joinToString(" or ")}
            4- ${request.songs.joinToString(" or ")}                  
        """.trimIndent()

    }
}