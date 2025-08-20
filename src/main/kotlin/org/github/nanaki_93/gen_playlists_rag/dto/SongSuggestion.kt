package org.github.nanaki_93.gen_playlists_rag.dto

import org.github.nanaki_93.gen_playlists_rag.data.Song

data class SongSearchRequest(
    val limit: Int? = null,
    val songs: List<String>,
    val artists: List<String>,
    val genres: List<String>,
    val years: List<String>,
)
data class SongSuggestionRequest(
    val query: String,
)

data class SongSearchResponse(
    val results: List<Song>
)

