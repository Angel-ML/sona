package com.tencent.angel.ps.apis;

import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.psagent.matrix.MatrixClient;

import java.util.HashMap;
import java.util.List;

public class PSWokerEntryPoint {
    private PSAgent psAgent;
    private HashMap<String, MatrixClient> matClientMap;

    public byte[] pull(String name) {
        MatrixClient matrixClient;
        if (matClientMap.containsKey(name)) {
            matrixClient = matClientMap.get(name);
        } else {
            psAgent.getMatrixClient()
            matrixClient = psAgent.getMatrixClient(name);
            matClientMap.put(name, matrixClient);
        }

        matrixClient.getRow(1);

        return null;

    }

    public static void main(String[] args){

    }
}
