package io.shardingsphere.transaction.xa.narayana;

import com.arjuna.ats.arjuna.recovery.RecoveryManager;
import com.arjuna.ats.internal.jta.recovery.arjunacore.XARecoveryModule;
import com.arjuna.ats.jbossatx.jta.RecoveryManagerService;
import com.arjuna.ats.jta.common.jtaPropertyManager;
import lombok.SneakyThrows;
import org.apache.shardingsphere.transaction.xa.spi.SingleXAResource;
import org.apache.shardingsphere.transaction.xa.spi.XATransactionManager;

import javax.sql.XADataSource;
import javax.transaction.TransactionManager;

/**
 * Narayana Transaction Manager
 *
 * @author zhfeng
 */
public final class NarayanaXATransactionManager implements XATransactionManager {

    private static final TransactionManager TRANSACTION_MANAGER = jtaPropertyManager.getJTAEnvironmentBean().getTransactionManager();
    private static final XARecoveryModule xaRecoveryModule = XARecoveryModule.getRegisteredXARecoveryModule();
    private static final RecoveryManagerService recoveryManagerService = new RecoveryManagerService();

    @Override
    public void init() {
        RecoveryManager.delayRecoveryManagerThread();
        recoveryManagerService.create();
        recoveryManagerService.start();
    }

    @SneakyThrows
    @Override
    public void registerRecoveryResource(String dataSourceName, XADataSource xaDataSource) {
        xaRecoveryModule.addXAResourceRecoveryHelper(new DataSourceXAResourceRecoveryHelper(xaDataSource));
    }

    @SneakyThrows
    @Override
    public void removeRecoveryResource(String dataSourceName, XADataSource xaDataSource) {
        xaRecoveryModule.removeXAResourceRecoveryHelper(new DataSourceXAResourceRecoveryHelper(xaDataSource));
    }

    @SneakyThrows
    @Override
    public void enlistResource(SingleXAResource singleXAResource) {
        TRANSACTION_MANAGER.getTransaction().enlistResource(singleXAResource.getDelegate());
    }

    @Override
    public TransactionManager getTransactionManager() {
        return TRANSACTION_MANAGER;
    }

    @Override
    public void close() throws Exception {
        recoveryManagerService.stop();
        recoveryManagerService.destroy();
    }
}
