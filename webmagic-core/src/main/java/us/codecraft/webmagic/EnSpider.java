package us.codecraft.webmagic;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.SpiderListener;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.downloader.Downloader;
import us.codecraft.webmagic.downloader.HttpClientDownloader;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.scheduler.QueueScheduler;
import us.codecraft.webmagic.scheduler.Scheduler;
import us.codecraft.webmagic.thread.CountableThreadPool;
import us.codecraft.webmagic.utils.UrlUtils;

public class EnSpider implements Runnable, Task {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	//下载器
	protected Downloader downloader;
	//持久化
	protected List<Pipeline> pipelines = new ArrayList<Pipeline>();
	//种子请求
	protected List<Request> startRequests;
	//页面解析器
	protected PageProcessor pageProcessor;
	//浏览器属性
	protected Site site;
	//任务标识
	protected String uuid;
	//调度器，内部实现一个push和poll方法，其实是一个队列的数据结构
	protected Scheduler scheduler=new QueueScheduler();
	//线程池
	protected CountableThreadPool threadPool;
	//线程执行器
	protected ExecutorService executorService;
	//线程数，控制爬取线程
	protected int threadNum = 1;

	//爬虫状态标识
	protected final static int STAT_INIT = 0;//初始化
	protected final static int STAT_RUNNING = 1;//运行中
	protected final static int STAT_STOPPED = 2;//结束
	protected AtomicInteger stat = new AtomicInteger(STAT_INIT);

	//退出时回收操作
	protected boolean destroyWhenExit = true;

	//是否回流url,spawn产卵。个人觉得这个参数很多余
	protected boolean spawnUrl = true;
	
	//采集完成退出
	protected boolean exitWhenComplete = true;

	//控制新生成url锁
	protected ReentrantLock newUrlLock = new ReentrantLock();
	protected Condition newUrlCondition = newUrlLock.newCondition();
	//调度器空闲时，等待时间
	protected int emptySleepTime = 30000;
	
	//监听器集合，请求爬去成功或者失败时，可以通过注入监听器分布实现onSuccess和onError方法
	protected List<SpiderListener> spiderListeners;
	//采集页面数统计（只代表请求的次数，不代表成功抓取数）
	protected final AtomicLong pageCount = new AtomicLong(0);
	//爬取开始时间
	protected Date startTime;

	public EnSpider(PageProcessor pageProcessor) {
		this.pageProcessor = pageProcessor;
		this.site = pageProcessor.getSite();
	}

	public EnSpider startUrls(List<String> startUrls) {
		checkIfRunning();
		this.startRequests = UrlUtils.convertToRequests(startUrls);
		return this;
	}

	public EnSpider startRequest(List<Request> startRequests) {
		checkIfRunning();
		this.startRequests = startRequests;
		return this;
	}

	public EnSpider setUuid(String uuid) {
		this.uuid = uuid;
		return this;
	}

	public EnSpider setScheduler(Scheduler scheduler) {
		checkIfRunning();
		Scheduler oldScheduler = this.scheduler;
		this.scheduler = scheduler;
		if (oldScheduler != null) {
			Request request;
			while ((request = oldScheduler.poll(this)) != null) {
				this.scheduler.push(request, this);
			}
		}
		return this;
	}

	public EnSpider addPipeline(Pipeline pipeline) {
		checkIfRunning();
		this.pipelines.add(pipeline);
		return this;
	}

	public EnSpider setPipelines(List<Pipeline> pipelines) {
		checkIfRunning();
		this.pipelines = pipelines;
		return this;
	}

	public EnSpider clearPipeline() {
		pipelines = new ArrayList<Pipeline>();
		return this;
	}

	public EnSpider setDownloader(Downloader downloader) {
		checkIfRunning();
		this.downloader = downloader;
		return this;
	}

	protected void initComponent() {
		if (downloader == null) {
			this.downloader = new HttpClientDownloader();
		}
		if (pipelines.isEmpty()) {
			pipelines.add(new ConsolePipeline());
		}
		downloader.setThread(threadNum);
		if (threadPool == null || threadPool.isShutdown()) {
			if (executorService != null && !executorService.isShutdown()) {
				threadPool = new CountableThreadPool(threadNum, executorService);
			} else {
				threadPool = new CountableThreadPool(threadNum);
			}
		}
		if (startRequests != null) {
			for (Request request : startRequests) {
				addRequest(request);
			}
			startRequests.clear();
		}
		startTime = new Date();
	}

	@Override
	public void run() {
		checkRunningStat();
		initComponent();
		logger.info("Spider {} started!", getUUID());
		while (!Thread.currentThread().isInterrupted() && stat.get() == STAT_RUNNING) {
			final Request request = scheduler.poll(this);
			if (request == null) {
				
				if (threadPool.getThreadAlive() == 0 && exitWhenComplete) {
					//如果目前没有更多正在执行的线程了，那不会再生成新的url,就表示爬取结束了。很关键的逻辑
					break;
				}
				//目前还有在执行的线程，则等待，最长等待30秒，可能产生新的URL。
				waitNewUrl();
			} else {
				threadPool.execute(new Runnable() {
					@Override
					public void run() {
						try {
							processRequest(request);
							onSuccess(request);
						} catch (Exception e) {
							onError(request);
							logger.error("process request " + request + " error", e);
						} finally {
							pageCount.incrementAndGet();
							signalNewUrl();//执行完，就唤醒，不用等30秒，因为可能已经产生了新的URL
						}
					}
				});
			}
		}
		stat.set(STAT_STOPPED);
		// release some resources
		if (destroyWhenExit) {
			close();
		}
		logger.info("Spider {} closed! {} pages downloaded.", getUUID(), pageCount.get());
	}

	private void processRequest(Request request) {
		Page page = downloader.download(request, this);
		if (page.isDownloadSuccess()) {
			onDownloadSuccess(request, page);
		} else {
			onDownloaderFail(request);
		}
	}

	private void signalNewUrl() {
		try {
			newUrlLock.lock();
			newUrlCondition.signalAll();
		} finally {
			newUrlLock.unlock();
		}
	}

	private void checkRunningStat() {
		while (true) {
			int statNow = stat.get();
			if (statNow == STAT_RUNNING) {
				throw new IllegalStateException("Spider is already running!");
			}
			if (stat.compareAndSet(statNow, STAT_RUNNING)) {
				break;
			}
		}
	}

	private void onDownloadSuccess(Request request, Page page) {
		if (site.getAcceptStatCode().contains(page.getStatusCode())) {
			//Page的http状态为  可以接受的状态时才执行解析网页（）
			pageProcessor.process(page);
			extractAndAddRequests(page, spawnUrl);
			if (!page.getResultItems().isSkip()) {
				for (Pipeline pipeline : pipelines) {
					pipeline.process(page.getResultItems(), this);
				}
			}
		} else {
			logger.info("page status code error, page {} , code: {}", request.getUrl(), page.getStatusCode());
		}
		sleep(site.getSleepTime());
		return;
	}

	protected void extractAndAddRequests(Page page, boolean spawnUrl) {
		if (spawnUrl && CollectionUtils.isNotEmpty(page.getTargetRequests())) {
			for (Request request : page.getTargetRequests()) {
				addRequest(request);
			}
		}
	}

	private void onDownloaderFail(Request request) {
		if (site.getCycleRetryTimes() == 0) {
			sleep(site.getSleepTime());
		} else {
			// for cycle retry
			doCycleRetry(request);
		}
	}

	private void doCycleRetry(Request request) {
		Object cycleTriedTimesObject = request.getExtra(Request.CYCLE_TRIED_TIMES);
		if (cycleTriedTimesObject == null) {
			addRequest(SerializationUtils.clone(request).setPriority(0).putExtra(Request.CYCLE_TRIED_TIMES, 1));
		} else {
			int cycleTriedTimes = (Integer) cycleTriedTimesObject;
			cycleTriedTimes++;
			if (cycleTriedTimes < site.getCycleRetryTimes()) {
				addRequest(SerializationUtils.clone(request).setPriority(0).putExtra(Request.CYCLE_TRIED_TIMES,
						cycleTriedTimes));
			}
		}
		sleep(site.getRetrySleepTime());
	}

	private void sleep(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			logger.error("Thread interrupted when sleep", e);
		}
	}

	private void waitNewUrl() {
		newUrlLock.lock();
		try {
			// double check 二次
			if (threadPool.getThreadAlive() == 0 && exitWhenComplete) {
				return;
			}
			//
			newUrlCondition.await(emptySleepTime, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			logger.warn("waitNewUrl - interrupted, error {}", e);
		} finally {
			newUrlLock.unlock();
		}
	}

	protected void onError(Request request) {
		if (CollectionUtils.isNotEmpty(spiderListeners)) {
			for (SpiderListener spiderListener : spiderListeners) {
				spiderListener.onError(request);
			}
		}
	}

	protected void onSuccess(Request request) {
		if (CollectionUtils.isNotEmpty(spiderListeners)) {
			for (SpiderListener spiderListener : spiderListeners) {
				spiderListener.onSuccess(request);
			}
		}
	}

	public EnSpider addRequest(Request... requests) {
		for (Request request : requests) {
			addRequest(request);
		}
		signalNewUrl();
		return this;
	}

	private void addRequest(Request request) {
		if (site.getDomain() == null && request != null && request.getUrl() != null) {
			site.setDomain(UrlUtils.getDomain(request.getUrl()));
		}
		scheduler.push(request, this);
	}

	public void close() {
		destroyEach(downloader);
		destroyEach(pageProcessor);
		destroyEach(scheduler);
		for (Pipeline pipeline : pipelines) {
			destroyEach(pipeline);
		}
		threadPool.shutdown();
	}

	private void destroyEach(Object object) {
		if (object instanceof Closeable) {
			try {
				((Closeable) object).close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	protected void checkIfRunning() {
		if (stat.get() == STAT_RUNNING) {
			throw new IllegalStateException("Spider is already running!");
		}
	}

	@Override
	public String getUUID() {
		if (uuid != null) {
			return uuid;
		}
		if (site != null) {
			return site.getDomain();
		}
		uuid = UUID.randomUUID().toString();
		return uuid;
	}

	public Site getSite() {
		// TODO Auto-generated method stub
		return site;
	}

	public void start() {
		Thread thread = new Thread(this);
		thread.setDaemon(false);
		thread.start();
	}

	public EnSpider setThreadNum(int threadNum) {
		this.threadNum = threadNum;
		return this;
	}

	public EnSpider setExitWhenComplete(boolean exitWhenComplete) {
		this.exitWhenComplete = exitWhenComplete;
		return this;
	}

	public EnSpider setSpawnUrl(boolean spawnUrl) {
		this.spawnUrl = spawnUrl;
		return this;
	}

	public EnSpider setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
		return this;
	}

	public EnSpider setSpiderListeners(List<SpiderListener> spiderListeners) {
		this.spiderListeners = spiderListeners;
		return this;
	}

}
