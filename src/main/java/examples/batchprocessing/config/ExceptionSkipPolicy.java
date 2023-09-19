package examples.batchprocessing.config;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;

import java.text.ParseException;

public class ExceptionSkipPolicy implements SkipPolicy {


    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {
        return t instanceof ParseException;
    }
}