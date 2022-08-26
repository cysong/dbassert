package com.github.cysong.dbassert;

import com.github.cysong.dbassert.report.Reporter;
import com.github.cysong.dbassert.report.Status;
import io.qameta.allure.Allure;
import io.qameta.allure.model.StepResult;

import java.util.UUID;

import static io.qameta.allure.util.ResultsUtils.getStatus;
import static io.qameta.allure.util.ResultsUtils.getStatusDetails;

/**
 * reporter implements for allure
 *
 * @author cysong
 * @date 2022/8/26 11:34
 **/
public class AllureReporter implements Reporter {
    private static final ThreadLocal<String> UUIDS = new InheritableThreadLocal<>();

    @Override
    public void startStep(String name) {
        String uuid = UUID.randomUUID().toString();
        UUIDS.set(uuid);
        Allure.getLifecycle().startStep(uuid, (new StepResult()).setName(name).setStatus(io.qameta.allure.model.Status.PASSED));
    }

    @Override
    public void endStep(Status status) {
        String uuid = getUUID();
        Allure.getLifecycle().updateStep(uuid, (step) -> {
            step.setStatus(mapStatus(status));
        });
        Allure.getLifecycle().stopStep(uuid);
    }

    @Override
    public void endStep(Throwable throwable) {
        String uuid = getUUID();
        Allure.getLifecycle().updateStep(uuid, (step) -> {
            step.setStatus(getStatus(throwable).orElse(io.qameta.allure.model.Status.BROKEN))
                    .setStatusDetails(getStatusDetails(throwable).orElse(null));
        });
        Allure.getLifecycle().stopStep(uuid);
    }

    @Override
    public void addAttachment(String name, String content) {
        Allure.addAttachment(name, content);
    }

    @Override
    public void addAttachment(String name, String type, String content, String extension) {
        Allure.addAttachment(name, type, content, extension);
    }

    private io.qameta.allure.model.Status mapStatus(Status status) {
        return io.qameta.allure.model.Status.fromValue(status.value);
    }

    private String getUUID() {
        String uuid = UUIDS.get();
        if (uuid != null) {
            return uuid;
        }
        throw new IllegalStateException("UUID has not set");
    }
}
