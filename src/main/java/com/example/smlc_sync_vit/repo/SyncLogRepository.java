package com.example.smlc_sync_vit.repo;

import com.example.smlc_sync_vit.model.SyncLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface SyncLogRepository extends JpaRepository<SyncLog, Long> {

    @Query(value = "select * from SYNC_VIT_LOG u order by sync_finished_at desc limit 1", nativeQuery = true)
    Optional<SyncLog> findLatestSyncLocal();
}
