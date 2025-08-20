package org.github.nanaki_93.gen_playlists_rag.config

import io.qdrant.client.QdrantClient
import io.qdrant.client.QdrantGrpcClient
import org.springframework.ai.embedding.EmbeddingModel
import org.springframework.ai.vectorstore.qdrant.QdrantVectorStore
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class VectorStoreConfig {
    @Bean
    fun qdrantClient(): QdrantClient {
        return QdrantClient(
            QdrantGrpcClient.newBuilder("localhost", 6334, false).build()
        )
    }

    @Bean
    fun vectorStore(embeddingModel: EmbeddingModel, qdrantClient: QdrantClient): QdrantVectorStore {
        return QdrantVectorStore.builder(qdrantClient, embeddingModel)
            .collectionName("songs_collection")
            .initializeSchema(true)
            .build()
    }

}