package com.tencent.angel.ps.apis;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.yarn.AngelYarnClient;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.ModelLoadContext;
import com.tencent.angel.model.ModelSaveContext;
import com.tencent.angel.ps.tensor.TensorMeta;
import com.tencent.angel.ps.variable.VariableMeta;
import com.tencent.angel.ps.variable.Variable;
import org.apache.hadoop.conf.Configuration;
import py4j.GatewayServer;

import java.util.*;

public class PSMasterEnterPoint {
    private AngelClient angelClient;
    private Map<String, TensorMeta> matMeta;
    private Map<String, VariableMeta> varMeta;
    private Map<String, Status> status;
    private Location location;

    private PSMasterEnterPoint() {
        Configuration conf = new Configuration();
        angelClient = new AngelYarnClient(conf);
        angelClient.startPSServer();

        location = angelClient.getMasterLocation();

        matMeta = new HashMap<String, TensorMeta>();
        varMeta = new HashMap<String, VariableMeta>();
        status = new HashMap<String, Status>();
    }

    public void addTensor(String name, String dtype, int dim, long[] shape, long validIndexNum) throws Exception {

        TensorMeta meta = new TensorMeta(name, dtype, dim, shape, validIndexNum);
        status.put(name, Status.Added);
        matMeta.put(name, meta);
    }

    public void createMatrix(String name, String dtype, int numRows, long numCols, long validIndexNum) throws Exception {
        if (matMeta.containsKey(name) || status.get(name) == Status.Created) {
            throw new Exception("the matrix " + name + " has created!");
        } else if (matMeta.containsKey(name) && status.get(name) == Status.Added) {
            TensorMeta meta = matMeta.get(name);
            TensorMeta newMeta = new TensorMeta(name, dtype, numRows, numCols, validIndexNum);
            assert meta.equals(newMeta);
            status.put(name, Status.Created);
            angelClient.createMatrices(Collections.singletonList(meta.getMatrixContext()));
        } else {
            TensorMeta meta = new TensorMeta(name, dtype, numRows, numCols, validIndexNum);
            matMeta.put(name, meta);
            status.put(name, Status.Created);
            angelClient.createMatrices(Collections.singletonList(meta.getMatrixContext()));
        }
    }

    public void addVariable(String name, String dtype, int numRows, long numCols, long validIndexNum,
                            Map<String, String> updater, String formatClassName, Boolean allowPullWithIndex) {
        int numSlot = Integer.parseInt(updater.getOrDefault("numSlot", "0"));
        VariableMeta meta = new VariableMeta(name, dtype, numRows, numCols, validIndexNum, updater)
        status.put(name, Status.Added);
        varMeta.put(null, null);
    }

    public void createVariable(String name, String dtype, int numRows, long numCols, long validIndexNum,
                               Map<String, String> updater, String formatClassName, Boolean allowPullWithIndex) {

        status.put(name, Status.Created);
        varMeta.put(null, null);
    }

    public void createAddedMatrixAndVariable() {
        List<MatrixContext> matContexts = new ArrayList<MatrixContext>(matMeta.values());
        for (Map.Entry<String, Variable> entry : varMeta.entrySet()) {
            String name = entry.getKey();
            if (status.get(name) == Status.Added) {
                matContexts.add(entry.getValue().matrixContext());
            }
        }
        angelClient.createMatrices(matContexts);
        for (MatrixContext ctx : matContexts) {
            status.put(ctx.getName(), Status.Created);
        }
    }

    public void loadModel(String path) {
        ModelLoadContext modelLoadContext = new ModelLoadContext();
        for (Map.Entry<String, Variable> entry : varMeta.entrySet()) {
            String name = entry.getKey();
            if (status.get(name) == Status.Created) {
                modelLoadContext.addMatrix(entry.getValue().matrixLoadContext(path));
            } else {
                System.out.println("variable is not create, cannot load!");
            }
        }

        angelClient.load(modelLoadContext);
    }

    public void saveModel(String path, String formatClassName) {
        List<MatrixSaveContext> matricesContext = new ArrayList<MatrixSaveContext>();

        for (Map.Entry<String, Variable> entry : varMeta.entrySet()) {
            String name = entry.getKey();
            if (status.get(name) == Status.Created) {
                matricesContext.add(entry.getValue().matrixSaveContext(path, formatClassName));
            } else {
                System.out.println("variable is not create, cannot save!");
            }
        }

        ModelSaveContext modelSaveContext = new ModelSaveContext(path, matricesContext);
        angelClient.save(modelSaveContext);
    }

    public String getMasterIP() {
        return location.getIp();
    }

    public int getMasterPort() {
        return location.getPort();
    }

    public static void main() {
        try {
            PSMasterEnterPoint app = new PSMasterEnterPoint();
            GatewayServer server = new GatewayServer(app);
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
