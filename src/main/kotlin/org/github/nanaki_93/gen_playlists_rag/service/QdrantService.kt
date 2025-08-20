package org.github.nanaki_93.gen_playlists_rag.service

import io.qdrant.client.QdrantClient
import org.springframework.ai.document.Document
import org.springframework.ai.vectorstore.VectorStore
import org.springframework.stereotype.Service

@Service
class QdrantService(private val vectorStore: VectorStore, private val qdrantClient: QdrantClient) {
    private val collectionName = "songs_collection"

    fun isVectorStoreEmpty(): Boolean {
        return try {
            val collectionInfo = qdrantClient.getCollectionInfoAsync(collectionName).get()
            collectionInfo.vectorsCount == 0L
        } catch (e: Exception) {
            println("Error checking collection: ${e.message}")
            true // Assume empty if collection doesn't exist
        }
    }

    fun getVectorStoreSize(): Long {
        return try {
            val collectionInfo = qdrantClient.getCollectionInfoAsync(collectionName).get()
            collectionInfo.pointsCount
        } catch (e: Exception) {
            println("Error getting collection size: ${e.message}")
            0L
        }
    }

    fun collectionExists(): Boolean {
        return try {
            val collections = qdrantClient.listCollectionsAsync().get()
            collections.contains(collectionName)
        } catch (e: Exception) {
            false
        }
    }
    fun addDocuments(documents: List<Document>){
        vectorStore.add(documents)
    }
}

