package org.apache.sysds.runtime.meta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysds.parser.StatementBlock;
import org.apache.sysds.runtime.io.IOUtilFunctions;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.HDFSTool;
import scala.Tuple2;

import java.io.*;
import java.nio.IntBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 扩展元数据
 */
public class MetaDataExt {
    private static final Log LOG = LogFactory.getLog(StatementBlock.class.getName());

    public static final Map<String, MetaDataExt> CACHE = new ConcurrentHashMap<>();

    public static final String R = "rNnz";
    public static final String C = "cNnz";
    public static final String EXT = ".ext";

    public static final String DELIMITER = "\n";
    public static final String SPLITTER = "&&";

    public final EstimatorMatrixHistogram.MatrixHistogram e;

    public MetaDataExt(EstimatorMatrixHistogram.MatrixHistogram e) {
        this.e = e;
    }

    public MetaDataExt(MatrixBlock m) {
        this.e = new EstimatorMatrixHistogram.MatrixHistogram(m, false);
    }

    public MetaDataExt(int[] r, int[] c) {
        e = new EstimatorMatrixHistogram.MatrixHistogram(r, c);
    }

    public static MetaDataExt readAndParse(String filename, long rows, long cols) {
        if (!HDFSTool.existsFileOnHDFS(filename)) {
            return null;
        }

        long start = System.currentTimeMillis();

        int[] r = new int[(int) rows];
        int[] c = new int[(int) cols];

        int rIdx = 0;
        int cIdx = 0;

        if (HDFSTool.isDirectory(filename)) { // 目录
            List<Path> pathList = Arrays.stream(HDFSTool.getDirectoryListing(filename))
                    .map(FileStatus::getPath)
                    .filter(e -> e.getName().startsWith("part"))
                    .sorted(Comparator.comparing(Path::getName))
                    .collect(Collectors.toList());
            boolean rowLine = true;
            for (Path path : pathList) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(IOUtilFunctions.getFileSystem(path).open(path)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (Objects.equals(line, SPLITTER)) {
                            rowLine = false;
                            continue;
                        }
                        if (rowLine) {
                            r[rIdx++] = Integer.parseInt(line);
                        } else {
                            c[cIdx++] = Integer.parseInt(line);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } else { // 文件
            Path path = new Path(filename);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(IOUtilFunctions.getFileSystem(path).open(path)))) {
                String line;
                while (!Objects.equals((line = br.readLine()), SPLITTER)) {
                    r[rIdx++] = Integer.parseInt(line);
                }
                while ((line = br.readLine()) != null) {
                    c[cIdx++] = Integer.parseInt(line);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        long end = System.currentTimeMillis();

        LOG.info("read meta data extension file [" + filename + "] time: " + (end - start) + "ms");

//        LOG.debug(Arrays.toString(r));
//        LOG.debug(Arrays.toString(c));

        return new MetaDataExt(r, c);
    }

    public void write(String filename) {
        long start = System.currentTimeMillis();

        try {
            HDFSTool.deleteFileIfExistOnHDFS(filename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        int[] r = e.getRowCounts();
        int[] c = e.getColCounts();

        Path path = new Path(filename);
        try (BufferedWriter br = new BufferedWriter(new OutputStreamWriter(IOUtilFunctions.getFileSystem(path).create(path,true)))) {
            br.write(nnzCountsToString(r));
            br.write(DELIMITER);

            br.write(SPLITTER);
            br.write(DELIMITER);

            br.write(nnzCountsToString(c));
            br.write(DELIMITER);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long end = System.currentTimeMillis();

        LOG.info("write meta data extension file [" + filename + "] time: " + (end - start) + "ms");

//        LOG.debug(Arrays.toString(r));
//        LOG.debug(Arrays.toString(c));
    }

    public static String nnzCountsToString(int[] nnzCounts) {
        List<String> list = Arrays.stream(nnzCounts)
                .mapToObj(String::valueOf)
                .collect(Collectors.toList());
        return String.join(DELIMITER, list);
    }

    public static JavaRDD<String> nnzCountsToRDDString(JavaPairRDD<Long, int[]> pair) {
        return pair
                .reduceByKey((v1, v2) -> {
                    int[] ret = new int[v1.length];
                    for (int i = 0; i < v1.length; i++) {
                        ret[i] = v1[i] + v2[i];
                    }
                    return ret;
                })
                .sortByKey()
                .map(v -> MetaDataExt.nnzCountsToString(v._2));
    }

    public static int[] collectNnzCounts(JavaPairRDD<Long, int[]> pair) {
        List<int[]> collect = pair
                .reduceByKey((v1, v2) -> {
                    int[] ret = new int[v1.length];
                    for (int i = 0; i < v1.length; i++) {
                        ret[i] = v1[i] + v2[i];
                    }
                    return ret;
                })
                .sortByKey()
                .map(v1 -> v1._2)
                .collect();
        int n = collect.stream()
                .mapToInt(e -> e.length)
                .sum();
        IntBuffer buffer = IntBuffer.allocate(n);
        for (int[] array : collect) {
            buffer.put(array);
        }
        return buffer.array();
    }
}
