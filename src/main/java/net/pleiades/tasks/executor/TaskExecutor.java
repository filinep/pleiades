/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.tasks.executor;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import java.util.Map;
import java.util.Properties;
import net.pleiades.Config;
import net.pleiades.State;
import static net.pleiades.State.*;
import net.pleiades.Utils;
import net.pleiades.tasks.Task;

/**
 * @author bennie
 */
//public class TaskExecutor implements Executor, Runnable, MessageListener<Map<String, Task>> {
//    protected State state;
//    protected SimulationSelector simulationSelector;
//    protected Task currentTask;
//    protected boolean running;
//    protected Properties properties;
//    protected ITopic tasksTopic, requestTopic, resultsTopic, errorTopic;
//    protected String id;
//    protected boolean requestSent;
//    protected boolean success;
//
//    public TaskExecutor(Properties properties, SimulationSelector jobSelector, String id) {
//        this.id = Hazelcast.getCluster().getLocalMember().getInetSocketAddress().toString() + "-" + id;
//        this.requestSent = false;
//        this.state = State.IDLE;
//        this.simulationSelector = jobSelector;
//        this.running = false;
//        this.properties = properties;
//        this.tasksTopic = Hazelcast.getTopic(Config.tasksTopic);
//        this.requestTopic = Hazelcast.getTopic(Config.requestTopic);
//        this.resultsTopic = Hazelcast.getTopic(Config.resultsTopic);
//        this.errorTopic = Hazelcast.getTopic(Config.errorTopic);
//        addListeners();
//    }
//
//    private void addListeners() {
//        tasksTopic.addMessageListener(this);
//    }
//
//    private void removeListeners() {
//        tasksTopic.removeMessageListener(this);
//    }
//
//    @Override
//    public void requestNewTask() {
//        currentTask = null;
//        state = State.REQUESTING;
//        requestTopic.publish(id);
//        requestSent = true;
//        state = State.REQUEST_SENT;
//    }
//
//    public void executeTask() {
//        //System.out.println(id + " is now executing " + currentTask.getId() + " !!!");
//        state = State.PREPARING;
//        String bin = currentTask.getId() + ".run";
//        currentTask.getParent().writeBinary(bin);
//        currentTask.writeFile();
//        state = State.EXECUTING;
//        success = currentTask.execute(properties);
//        currentTask.getParent().deleteBinary(bin);
//        currentTask.deleteFile();
//
//        completeTask(currentTask);
//    }
//
//    public void completeTask(Task t) {
//        currentTask = null;
//        state = State.COMPLETING;
//        if (success) {
//            resultsTopic.publish(t);
//        } else {
//            if (!t.getOutput().isEmpty()) {
//                System.out.println("Publishing Error.");
//                errorTopic.publish(t);
//            }
//        }
//        requestSent = false;
//        state = State.COMPLETED;
//    }
//
//    @Override
//    public synchronized void onMessage(Message<Map<String, Task>> message) {
//        if (message.getMessageObject().isEmpty()) {
//            if ((state == State.REQUEST_SENT || state == State.IDLE) && requestSent) {
//                System.out.println(id + " got empty message!");
//                requestSent = false;
//            }
//        } else {
//            if (message.getMessageObject().keySet().toArray()[0].equals(id)) {
//                state = State.JOB_RECEIVED;
//                currentTask = message.getMessageObject().get(id);
//            }
//        }
//    }
//
//    public State getState() {
//        return state;
//    }
//
//    @Override
//    public void run() {
//        this.running = true;
//
//        while (true) {
//            if (running || currentTask != null) {
//                //System.out.println(id + " is running.");
//                if (currentTask != null) {
//                    executeTask();
//                    state = State.IDLE;
//                } else if (!requestSent && running) {
//                    requestNewTask();
//                }
//            }
//            Utils.sleep(5000);
//        }
//    }
//
//    @Override
//    public String getStateString() {
//        String taskName, progress;
//        if (currentTask == null || state == State.REQUESTING || state == State.IDLE) {
//            taskName = "(No task)";
//            progress = "(No progress)";
//
//        } else {
//            taskName = currentTask.getId();
//            progress = currentTask.getProgress();
//        }
//        
//        return state.name() + " " + taskName + "\n" + progress;
//    }
//
//    @Override
//    public void stop() {
//        //System.out.println(id + " is stopping");
//        removeListeners();
//        this.running = false;
//    }
//
//    @Override
//    public void start() {
//        //System.out.println(id + " is starting");
//        addListeners();
//        this.running = true;
//
//        if (state == State.REQUEST_SENT || state == State.IDLE) {
//            requestSent = false;
//        }
//    }
//
//    @Override
//    public void toggle() {
//        if (running) {
//            stop();
//        } else {
//            start();
//        }
//    }
//}

public class TaskExecutor implements Executor, Runnable, MessageListener<Map<String, Task>> {
    protected ITopic TASKS;
    protected ITopic REQUESTS;
    protected ITopic RESULTS;
    protected ITopic ERRORS;
    
    protected State state;
    protected Task currentTask;
    protected boolean running;
    protected Properties properties;
    protected String id;

    public TaskExecutor(Properties properties, String id) {
        this.TASKS = Hazelcast.getTopic(Config.tasksTopic);
        this.REQUESTS = Hazelcast.getTopic(Config.requestTopic);
        this.RESULTS = Hazelcast.getTopic(Config.resultsTopic);
        this.ERRORS = Hazelcast.getTopic(Config.errorTopic);
        
        this.id = Hazelcast.getCluster().getLocalMember().getInetSocketAddress().toString() + "-" + id;
        this.state = IDLE;
        this.running = false;
        this.properties = properties;
        
        addListeners();
    }

    @Override
    public synchronized void requestNewTask() {
        state(REQUESTING);
        
        REQUESTS.publish(id);
        
        state(REQUEST_SENT);
    }

    public void executeTask() {
        state(PREPARING);
        
        String bin = currentTask.getId() + ".run";
        currentTask.getParent().writeBinary(bin);
        currentTask.writeFile();
        
        state(EXECUTING);
        
        boolean success = currentTask.execute(properties);
        currentTask.getParent().deleteBinary(bin);
        currentTask.deleteFile();

        state(COMPLETING);
        
        if (success) {
            RESULTS.publish(currentTask);
        } else if (!currentTask.getOutput().isEmpty()) {
            ERRORS.publish(currentTask);
        }
        
        state(COMPLETED);
        
        currentTask = null;
        
        state(IDLE);
    }

    @Override
    public synchronized void onMessage(Message<Map<String, Task>> message) {
        if (!message.getMessageObject().isEmpty()) {
            if (message.getMessageObject().keySet().contains(id)) {
                currentTask = message.getMessageObject().get(id);
                System.out.println("|| " + id + " got " + currentTask.getId());
                state(JOB_RECEIVED);
            }
        } else {
            if (isState(REQUEST_SENT)) {
                state(IDLE);
            }
        }
    }

    @Override
    public void run() {
        running = true;

        while (true) {
            Utils.sleep(5000);
            
            if (currentTask != null) {
                executeTask();
            } else if (running && !isState(REQUEST_SENT)) {
                requestNewTask();
            }
        }
    }

    @Override
    public String getStateString() {
        String taskName, progress;
        if (currentTask == null || isState(REQUESTING) || isState(IDLE)) {
            taskName = "(No task)";
            progress = "(No progress)";
        } else {
            taskName = currentTask.getId();
            progress = currentTask.getProgress();
        }
        
        return state.name() + " " + taskName + "\n" + progress;
    }

    @Override
    public void stop() {
        removeListeners();
        this.running = false;
    }

    @Override
    public void start() {
        addListeners();
        this.running = true;
    }

    @Override
    public void toggle() {
        if (running) {
            stop();
        } else {
            start();
        }
    }
    
    private void addListeners() {
        TASKS.addMessageListener(this);
    }

    private void removeListeners() {
        TASKS.removeMessageListener(this);
    }
    
    private boolean isState(State s) {
        return state == s;
    }
    
    private void state(State s) {
        state = s;
        System.out.println("|| " + id + " " + s);
    }
}
