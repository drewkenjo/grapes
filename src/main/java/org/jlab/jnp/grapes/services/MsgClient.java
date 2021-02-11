package org.jlab.jnp.grapes.services;

import org.json.JSONObject;
import com.google.gson.Gson;

import java.time.Duration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Arrays;

import org.jlab.groot.data.IDataSet;
import org.jlab.groot.data.H1F;
import org.jlab.groot.data.H2F;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;


public class MsgClient {

  private static RedisCommands<String, String> syncInstance;

  public static RedisCommands<String, String> connection() {
    if(syncInstance == null ) {
      RedisClient client = RedisClient.create("redis://127.0.0.1:6379");
      StatefulRedisConnection<String, String> connection = client.connect();
      syncInstance = connection.sync();
    }

    return syncInstance;
  }

  public static class Stage {
    ConcurrentHashMap<String, IDataSet> hists = new ConcurrentHashMap<>();
    String name;

    public Stage(String name) {
      this.name = name;
    }

    public Stage fill(String hname, double val) {
      IDataSet ds = hists.get(hname);
      if(ds instanceof H1F) {
        ((H1F) ds).fill(val);
      }
      return this;
    }

    public Stage fill(String hname, double uu, double vv) {
      IDataSet ds = hists.get(hname);
      if(ds instanceof H1F) {
        ((H1F) ds).fill(uu, vv);
      } else if(ds instanceof H2F) {
        ((H2F) ds).fill(uu, vv);
      }
      return this;
    }

    public Stage fill(String hname, double uu, double vv, double ww) {
      IDataSet ds = hists.get(hname);
      if(ds instanceof H2F) {
        ((H2F) ds).fill(uu, vv, ww);
      }
      return this;
    }

    public Stage register(IDataSet h1) {
      hists.put(h1.getName(), h1);
      return this;
    }
  }

/////////////////////////////////////////////////////
/////////////////////////////////////////////////////
//
  String username, flowname;
  ScheduledExecutorService executor;
  Map<String, Stage> stages = new ConcurrentHashMap<>();

  final JedisPoolConfig poolConfig = buildPoolConfig();
  JedisPool jedisPool = new JedisPool(poolConfig, "localhost");

  private JedisPoolConfig buildPoolConfig() {
    final JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(128);
    poolConfig.setMaxIdle(128);
    poolConfig.setMinIdle(16);
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(true);
    poolConfig.setTestWhileIdle(true);
    poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
    poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
    poolConfig.setNumTestsPerEvictionRun(3);
    poolConfig.setBlockWhenExhausted(true);
    return poolConfig;
  }

  public MsgClient(String username, String flowname) {
    this.username = username;
    this.flowname = flowname;
  }

  public static Map<String, Object> convertH1F(H1F h1) {
    Map<String, Object> out = new HashMap<>();
    List<List<Double>> data = new ArrayList<>();
    for(int ibin=0;ibin<h1.getDataSize(0);ibin++){
      data.add(Arrays.asList(h1.getDataX(ibin), h1.getDataY(ibin)));
    }
    out.put("name", h1.getName());
    String[] tls = h1.getTitle().split(";");
    out.put("title", tls.length>0 ? tls[0] : "");
    out.put("xtitle", tls.length>1 ? tls[1] : h1.getTitleX());
    out.put("ytitle", tls.length>2 ? tls[2] : h1.getTitleY());
    out.put("data", data);
    out.put("step", "center");
    out.put("xbins", h1.getAxis().getNBins());
    out.put("xmin", h1.getAxis().min());
    out.put("xmax", h1.getAxis().max());
    return out;
  }


  public static Map<String, Object> convertH2F(H2F h2) {
    Map<String, Object> out = new HashMap<>();
    List<List<Double>> data = new ArrayList<>();

    for(int ix=0;ix<h2.getDataSize(0);ix++)
    for(int iy=0;iy<h2.getDataSize(1);iy++){
      if(h2.getBinContent(ix, iy)!=0)
        data.add(Arrays.asList(h2.getDataX(ix), h2.getDataY(iy), h2.getBinContent(ix, iy)));
    }
    out.put("name", h2.getName());
    String[] tls = h2.getTitle().split(";");
    out.put("title", tls.length>0 ? tls[0] : "");
    out.put("xtitle", tls.length>1 ? tls[1] : h2.getTitleX());
    out.put("ytitle", tls.length>2 ? tls[2] : h2.getTitleY());
    out.put("min", h2.getMin());
    out.put("max", h2.getMax());
    out.put("xbins", h2.getXAxis().getNBins());
    out.put("xmin", h2.getXAxis().min());
    out.put("xmax", h2.getXAxis().max());
    out.put("ybins", h2.getYAxis().getNBins());
    out.put("ymin", h2.getYAxis().min());
    out.put("ymax", h2.getYAxis().max());
    out.put("colsize", h2.getDataEX(0));
    out.put("rowsize", h2.getDataEY(0));
    out.put("data", data);
    return out;
  }

  private void submit() {
    stages.entrySet().stream()
      .forEach(ee -> {
        try(Jedis jedis = jedisPool.getResource()) {
          jedis.sadd(username+"/"+flowname, ee.getKey());
        }

        ee.getValue().hists.values().stream()
          .forEach(ds -> {
            String jds = "";
            if(ds instanceof H1F) {
              jds = new JSONObject(convertH1F((H1F) ds)).toString();
//              connection().hset(username+"/"+flowname+"/"+ee.getKey(), ds.getName(), jds);
            } else if(ds instanceof H2F) {
              jds = new JSONObject(convertH2F((H2F) ds)).toString();
//              connection().hset(username+"/"+flowname+"/"+ee.getKey(), ds.getName(), jds);
            }

            if(!jds.isEmpty()) {
              try(Jedis jedis = jedisPool.getResource()) {
                jedis.hset(username+"/"+flowname+"/"+ee.getKey(), ds.getName(), jds);
              }
            }

          });
      });

  }
/*
  private void submit() {
    stages.values().forEach(stg -> stg.hists.forEach((k,h1) -> {
      StringBuilder sb = new StringBuilder();
      sb.append(h1.getXaxis().getNBins());
      sb.append(",");
      sb.append(h1.getXaxis().min());
      sb.append(",");
      sb.append(h1.getXaxis().max());
      sb.append(",");
      sb.append(IntStream.range(0, h1.getDataSize(0)).boxed()
        .map(ib -> h1.getDataX(ib)+","+h1.getDataY(ib))
        .collect( Collectors.joining(",") ));
      connection().set(username+"/"+flowname+"/"+stg.name+"/"+h1.getName(), sb.toString());
    }));
  }
*/

  public MsgClient enable() {
    executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleWithFixedDelay(() -> submit(), 5, 5, TimeUnit.SECONDS);

    return this;
  }

  public MsgClient stages(String... stgnames) {
    for(String stgname: stgnames) {
      Stage stg = new Stage(stgname);
      stages.put(stgname, stg);
    }
    return this;
  }

  public Stage stage(String name) {
    return stages.get(name);
  }

  public MsgClient fill(String hname, double val) {
    stages.values().forEach(stg -> stg.fill(hname, val));
    return this;
  }

  public MsgClient fill(String hname, double val, double weight) {
    stages.values().forEach(stg -> stg.fill(hname, val, weight));
    return this;
  }

  public void close() {
    executor.shutdown();
  }
}
