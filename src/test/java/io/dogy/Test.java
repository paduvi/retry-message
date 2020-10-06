package io.dogy;

import io.dogy.util.AbstractMessageQueueService;
import io.dogy.util.Message;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Test {

    public static class SimpleMessageQueueService extends AbstractMessageQueueService<String> {

        private static final ThreadPoolExecutor scheduler = new ThreadPoolExecutor(
                20, 20,
                10, TimeUnit.SECONDS, new LinkedBlockingQueue<>()
        );

        public SimpleMessageQueueService() {
            super(new Builder<>(String.class).withScheduler(scheduler));
        }

        @Override
        protected void processMessage(Message<String> message) throws Exception {
            Thread.sleep(ThreadLocalRandom.current().nextInt(50, 100));
            System.out.println(message.getPayload());
        }
    }

    public static void main(String[] args) throws Exception {
        SimpleMessageQueueService service = new SimpleMessageQueueService();

        for (int i = 0; i < 50; i++) {
            service.handleMessage(new Message<>("hello " + i));
        }
    }
}
