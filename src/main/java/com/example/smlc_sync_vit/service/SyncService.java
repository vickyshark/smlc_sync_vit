package com.example.smlc_sync_vit.service;

import com.example.smlc_sync_vit.model.SyncLog;
import com.example.smlc_sync_vit.repo.SyncLogRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.PropertySource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class SyncService implements CommandLineRunner {

    private static final String DATA_SETUP_NAME = "2.data_setup.sql";
    @Value("${base.path}")
    private String basePath;
    @Value("${sync.option}")
    private String syncOption;

    @Value("${sync.start.from.line}")
    private int startLine;

    private final JdbcTemplate jdbcTemplate;
    private final SyncLogRepository syncLogRepository;

    public SyncService(JdbcTemplate jdbcTemplate, SyncLogRepository syncLogRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.syncLogRepository = syncLogRepository;
    }

    @Override
    public void run(String... args) {
        log.info("Start sync...\nBase Path :: {}\nSyncOption :: {}\nStartLine :: {}", basePath, syncOption, startLine);
        if (StringUtils.isBlank(syncOption)) {
            log.warn("Invalid sync option, valid options are : CONTINUE, FROM_BUILD_Bxx");
            return;
        }

        if (syncOption.equals("CONTINUE")) {
            log.info("Continuing sync...");
            final Optional<SyncLog> latestSyncLocal = syncLogRepository.findLatestSyncLocal();
            if (latestSyncLocal.isEmpty()) {
                log.warn("Not found any previous sync. please choose FROM_BUILD_Bxx and FROM_LINE_xxx options. Exiting...");
                return;
            }

            final SyncLog latestSync = latestSyncLocal.get();
            final String buildVersion = latestSync.getBuildVersion();

            List<String> allLines;
            try {
                allLines = Files.readAllLines(Paths.get(getFilePath(buildVersion)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (latestSync.getLine() == allLines.size()) {
                final String nextBuild = getNextBuildVersion(latestSync.getBuildVersion());
                if (isBuildValid(nextBuild)) {
                    Pair<String, Long> syncResult = this.syncFromBuild(nextBuild);
                    log(syncResult);
                    return;
                }
                log.info("Nothing was left, all updated");
                return;
            }

            final long nextLine = latestSync.getLine() + 1;
            syncFromBuildAndLine(buildVersion, nextLine);
        }

        if (syncOption.startsWith("FROM_BUILD_B")) {
            final String buildVersion = extractBuildVersion(syncOption);
            if (!isBuildValid(buildVersion)) {
                throw new RuntimeException("Invalid build");
            }
            syncFromBuildAndLine(buildVersion, startLine);
        }
    }

    private void syncFromBuildAndLine(String buildVersion, long nextLine) {
        Pair<String, Long> syncResult;
        if (nextLine == 0) {
            syncResult = syncFromBuild(buildVersion);
            log(syncResult);
            return;
        }

        long lastRunLine = syncTheRest(buildVersion, nextLine);
        log.info("Sync the rest of {} finished, checking for new build...", buildVersion);
        final String nextBuild = getNextBuildVersion(buildVersion);
        if (!isBuildValid(nextBuild)) {
            log.info("Nothing was left, sync success");
            log(Pair.of(buildVersion, lastRunLine));
            return;
        }

        syncResult = this.syncFromBuild(nextBuild);
        log(syncResult);
    }

    private void log(Pair<String, Long> pair) {
        final SyncLog syncLog = SyncLog.builder()
                .buildVersion(pair.getKey())
                .line(pair.getValue())
                .syncFinishedAt(LocalDateTime.now())
                .build();

        syncLogRepository.save(syncLog);
        log.info("********** FINISHED **********");
        log.info("Sync log stored success full");
    }

    private boolean isBuildValid(String buildVersion) {
        final String folderPath = basePath + buildVersion.toUpperCase();
        final Path folder = Paths.get(folderPath);
        if (!Files.isDirectory(folder)) {
            log.warn("Build {} is not available, exiting", buildVersion.toUpperCase());
            return false;
        }

        final Path filePath = folder.resolve(DATA_SETUP_NAME);
        if (!Files.exists(filePath)) {
            log.error("Build {} is created but the {} is not exists", buildVersion.toUpperCase(), DATA_SETUP_NAME);
            return false;
        }

        return true;
    }

    private String extractBuildVersion(String str) {
        final Pattern pattern = Pattern.compile("B\\d+");
        final Matcher matcher = pattern.matcher(str);
        if (!matcher.find()) {
            throw new RuntimeException("Can not extract build version!");
        }
        return matcher.group();
    }

    public static String getNextBuildVersion(String buildVersion) {
        String numberPart = buildVersion.substring(1);
        try {
            return "B" + (Integer.parseInt(numberPart) + 1);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Parse failed");
        }
    }

    private int syncTheRest(String buildVersion, long fromLine) {

        final String filePath = getFilePath(buildVersion);
        try {
            List<String> allLines = Files.readAllLines(Paths.get(filePath));
            if (fromLine > allLines.size()) {
                log.error("Start line exceeds file length, nothing to run");
                throw new RuntimeException("Bad input");
            }

            String sqlScript = String.join("\n", allLines.subList((int) fromLine - 1, allLines.size()));
            jdbcTemplate.execute(sqlScript);
            return allLines.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getFilePath(String buildVersion) {
        return basePath + buildVersion.toUpperCase() + "/" + DATA_SETUP_NAME;
    }

    private Pair<String, Long> syncFromBuild(String buildVersion) {
        try (Stream<Path> fileStreams = Files.walk(Paths.get(basePath))) {
            List<Path> sqlFiles = fileStreams
                    .filter(path -> path.toString().endsWith("data_setup.sql"))
                    .filter(path -> extractNumber(path.toString()) >= extractNumber(buildVersion))
                    .sorted(Comparator.comparingInt(p -> extractNumber(p.toString())))
                    .collect(Collectors.toList());

            String lastRunBuild = buildVersion;
            long lastRunLine = 0;
            for (Path sqlFile : sqlFiles) {
                log.info("********** EXECUTING {} **********", sqlFile);
                try {
                    String sql = Files.readString(sqlFile, StandardCharsets.UTF_8);
                    jdbcTemplate.execute(sql);

                    lastRunBuild = extractBuildVersion(sqlFile.toString());
                    lastRunLine = Files.readAllLines(sqlFile).size();
                } catch (DataAccessException e) {
                    log.error("Sync stopped at build {}, line {} due to exception {}", lastRunBuild, lastRunLine, e.getCause().getMessage());
                    return Pair.of(lastRunBuild, lastRunLine);
                }
            }
            return Pair.of(lastRunBuild, lastRunLine);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static int extractNumber(String input) {
        final Pattern pattern = Pattern.compile("\\d+");
        final Matcher matcher = pattern.matcher(input);

        if (matcher.find()) {
            return Integer.parseInt(matcher.group());
        }
        throw new RuntimeException("No number founded from " + input);
    }

    @PostConstruct
    public void initSyncLogTable() {
        jdbcTemplate.execute("CREATE SEQUENCE IF NOT EXISTS SYNC_VIT_LOG_SEQ START 1 INCREMENT 1;\n" +
                "                CREATE TABLE IF NOT EXISTS SYNC_VIT_LOG (\n" +
                "                     id SERIAL PRIMARY KEY,\n" +
                "                     build_version VARCHAR,\n" +
                "                     line NUMERIC,\n" +
                "                     sync_finished_at TIMESTAMP\n" +
                "                 );");
    }
}
