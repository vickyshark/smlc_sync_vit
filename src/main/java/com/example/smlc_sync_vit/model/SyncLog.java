package com.example.smlc_sync_vit.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Data
@Entity
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "SYNC_VIT_LOG")
public class SyncLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(name = "build_version")
    private String buildVersion;

    @Column(name = "line")
    private long line;

    @Column(name = "sync_finished_at")
    private LocalDateTime syncFinishedAt;
}
