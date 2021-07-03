package com.kim.table.retract;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @Author: kim
 * @Description:
 * @Date: 2021/6/19 10:32
 * @Version: 1.0
 */
public class WebVisitSource extends RichParallelSourceFunction<WebVisit> {
	private Random r;
	private Boolean isCancel;
	private String[] broswerSeed;


	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		r = new Random();
		isCancel = false;
		broswerSeed = Stream.of("Chrome", "IE", "FireFox", "Safri").toArray(String[]::new);
	}

	@Override
	public void run(SourceContext<WebVisit> ctx) throws Exception {
		while (!isCancel) {
			WebVisit webVisit = new WebVisit();
			webVisit.setBrowser(broswerSeed[r.nextInt(broswerSeed.length)]);
			webVisit.setCookieId(UUID.randomUUID().toString());
			webVisit.setOpenTime(new Date());
			webVisit.setPageUrl("/pro/goods/" + UUID.randomUUID().toString() + ".html");
			webVisit.setIp(IntStream
					.range(1, 4)
					.boxed()
					.map(n -> (r.nextInt(255) + 2) % 255 + "")
					.collect(Collectors.joining(".")));

			ctx.collect(webVisit);
			TimeUnit.SECONDS.sleep(1);
		}
	}

	@Override
	public void cancel() {
		isCancel = true;

	}
}
