"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[64447],{13074:(e,n,i)=>{i.r(n),i.d(n,{assets:()=>l,contentTitle:()=>c,default:()=>u,frontMatter:()=>a,metadata:()=>t,toc:()=>d});const t=JSON.parse('{"id":"overview","title":"Overview","description":"Welcome to Apache Hudi! This overview will provide a high level summary of what Apache Hudi is and will orient you on","source":"@site/versioned_docs/version-0.14.1/overview.mdx","sourceDirName":".","slug":"/overview","permalink":"/cn/docs/0.14.1/overview","draft":false,"unlisted":false,"editUrl":"https://github.com/apache/hudi/tree/asf-site/website/versioned_docs/version-0.14.1/overview.mdx","tags":[],"version":"0.14.1","frontMatter":{"title":"Overview","keywords":["hudi","design","table","queries","timeline"],"summary":"Here we introduce some basic concepts & give a broad technical overview of Hudi","toc":true,"last_modified_at":"2019-12-30T19:59:57.000Z"},"sidebar":"docs","next":{"title":"Spark Guide","permalink":"/cn/docs/0.14.1/quick-start-guide"}}');var o=i(74848),s=i(28453),r=i(82915);const a={title:"Overview",keywords:["hudi","design","table","queries","timeline"],summary:"Here we introduce some basic concepts & give a broad technical overview of Hudi",toc:!0,last_modified_at:new Date("2019-12-30T19:59:57.000Z")},c=void 0,l={},d=[{value:"What is Apache Hudi",id:"what-is-apache-hudi",level:2},{value:"Core Concepts to Learn",id:"core-concepts-to-learn",level:2},{value:"Getting Started",id:"getting-started",level:2},{value:"Connect With The Community",id:"connect-with-the-community",level:2},{value:"Join in on discussions",id:"join-in-on-discussions",level:3},{value:"Come to Office Hours for help",id:"come-to-office-hours-for-help",level:3},{value:"Community Calls",id:"community-calls",level:3},{value:"Contribute",id:"contribute",level:2}];function h(e){const n={a:"a",code:"code",h2:"h2",h3:"h3",li:"li",p:"p",ul:"ul",...(0,s.R)(),...e.components};return(0,o.jsxs)(o.Fragment,{children:[(0,o.jsx)(n.p,{children:"Welcome to Apache Hudi! This overview will provide a high level summary of what Apache Hudi is and will orient you on\nhow to learn more to get started."}),"\n",(0,o.jsx)(n.h2,{id:"what-is-apache-hudi",children:"What is Apache Hudi"}),"\n",(0,o.jsxs)(n.p,{children:["Apache Hudi (pronounced \u201choodie\u201d) is the next generation ",(0,o.jsx)(n.a,{href:"/blog/2021/07/21/streaming-data-lake-platform",children:"streaming data lake platform"}),".\nApache Hudi brings core warehouse and database functionality directly to a data lake. Hudi provides ",(0,o.jsx)(n.a,{href:"/docs/next/sql_ddl",children:"tables"}),",\n",(0,o.jsx)(n.a,{href:"/docs/next/timeline",children:"transactions"}),", ",(0,o.jsx)(n.a,{href:"/docs/next/write_operations",children:"efficient upserts/deletes"}),", ",(0,o.jsx)(n.a,{href:"/docs/next/indexes",children:"advanced indexes"}),",\n",(0,o.jsx)(n.a,{href:"/docs/next/hoodie_streaming_ingestion",children:"streaming ingestion services"}),", data ",(0,o.jsx)(n.a,{href:"/docs/next/clustering",children:"clustering"}),"/",(0,o.jsx)(n.a,{href:"/docs/next/compaction",children:"compaction"})," optimizations,\nand ",(0,o.jsx)(n.a,{href:"/docs/next/concurrency_control",children:"concurrency"})," all while keeping your data in open source file formats."]}),"\n",(0,o.jsxs)(n.p,{children:["Not only is Apache Hudi great for streaming workloads, but it also allows you to create efficient incremental batch pipelines.\nRead the docs for more ",(0,o.jsx)(n.a,{href:"/docs/use_cases",children:"use case descriptions"})," and check out ",(0,o.jsx)(n.a,{href:"/powered-by",children:"who's using Hudi"}),", to see how some of the\nlargest data lakes in the world including ",(0,o.jsx)(n.a,{href:"https://eng.uber.com/uber-big-data-platform/",children:"Uber"}),", ",(0,o.jsx)(n.a,{href:"https://aws.amazon.com/blogs/big-data/how-amazon-transportation-service-enabled-near-real-time-event-analytics-at-petabyte-scale-using-aws-glue-with-apache-hudi/",children:"Amazon"}),",\n",(0,o.jsx)(n.a,{href:"http://hudi.apache.org/blog/2021/09/01/building-eb-level-data-lake-using-hudi-at-bytedance",children:"ByteDance"}),",\n",(0,o.jsx)(n.a,{href:"https://s.apache.org/hudi-robinhood-talk",children:"Robinhood"})," and more are transforming their production data lakes with Hudi."]}),"\n",(0,o.jsxs)(n.p,{children:["Apache Hudi can easily be used on any ",(0,o.jsx)(n.a,{href:"/docs/cloud",children:"cloud storage platform"}),".\nHudi\u2019s advanced performance optimizations, make analytical workloads faster with any of\nthe popular query engines including, Apache Spark, Flink, Presto, Trino, Hive, etc."]}),"\n",(0,o.jsx)(n.h2,{id:"core-concepts-to-learn",children:"Core Concepts to Learn"}),"\n",(0,o.jsx)(n.p,{children:"If you are relatively new to Apache Hudi, it is important to be familiar with a few core concepts:"}),"\n",(0,o.jsxs)(n.ul,{children:["\n",(0,o.jsxs)(n.li,{children:[(0,o.jsx)(n.a,{href:"/docs/next/timeline",children:"Hudi Timeline"})," \u2013 How Hudi manages transactions and other table services"]}),"\n",(0,o.jsxs)(n.li,{children:[(0,o.jsx)(n.a,{href:"/docs/next/storage_layouts",children:"Hudi File Layout"})," - How the files are laid out on storage"]}),"\n",(0,o.jsxs)(n.li,{children:[(0,o.jsx)(n.a,{href:"/docs/next/table_types",children:"Hudi Table Types"})," \u2013 ",(0,o.jsx)(n.code,{children:"COPY_ON_WRITE"})," and ",(0,o.jsx)(n.code,{children:"MERGE_ON_READ"})]}),"\n",(0,o.jsxs)(n.li,{children:[(0,o.jsx)(n.a,{href:"/docs/next/table_types#query-types",children:"Hudi Query Types"})," \u2013 Snapshot Queries, Incremental Queries, Read-Optimized Queries"]}),"\n"]}),"\n",(0,o.jsx)(n.p,{children:'See more in the "Concepts" section of the docs.'}),"\n",(0,o.jsxs)(n.p,{children:["Take a look at recent ",(0,o.jsx)(n.a,{href:"/blog",children:"blog posts"})," that go in depth on certain topics or use cases."]}),"\n",(0,o.jsx)(n.h2,{id:"getting-started",children:"Getting Started"}),"\n",(0,o.jsx)(n.p,{children:"Sometimes the fastest way to learn is by doing. Try out these Quick Start resources to get up and running in minutes:"}),"\n",(0,o.jsxs)(n.ul,{children:["\n",(0,o.jsxs)(n.li,{children:[(0,o.jsx)(n.a,{href:"/docs/quick-start-guide",children:"Spark Quick Start Guide"})," \u2013 if you primarily use Apache Spark"]}),"\n",(0,o.jsxs)(n.li,{children:[(0,o.jsx)(n.a,{href:"/docs/flink-quick-start-guide",children:"Flink Quick Start Guide"})," \u2013 if you primarily use Apache Flink"]}),"\n"]}),"\n",(0,o.jsx)(n.p,{children:"If you want to experience Apache Hudi integrated into an end to end demo with Kafka, Spark, Hive, Presto, etc, try out the Docker Demo:"}),"\n",(0,o.jsxs)(n.ul,{children:["\n",(0,o.jsx)(n.li,{children:(0,o.jsx)(n.a,{href:"/docs/docker_demo",children:"Docker Demo"})}),"\n"]}),"\n",(0,o.jsx)(n.h2,{id:"connect-with-the-community",children:"Connect With The Community"}),"\n",(0,o.jsx)(n.p,{children:"Apache Hudi is community focused and community led and welcomes new-comers with open arms. Leverage the following\nresources to learn more, engage, and get help as you get started."}),"\n",(0,o.jsx)(n.h3,{id:"join-in-on-discussions",children:"Join in on discussions"}),"\n",(0,o.jsxs)(n.p,{children:["See all the ways to ",(0,o.jsx)(n.a,{href:"/community/get-involved",children:"engage with the community here"}),". Two most popular methods include:"]}),"\n",(0,o.jsxs)(n.ul,{children:["\n",(0,o.jsxs)(n.li,{children:["\n",(0,o.jsx)(r.A,{title:"Hudi Slack Channel"}),"\n"]}),"\n",(0,o.jsxs)(n.li,{children:[(0,o.jsx)(n.a,{href:"mailto:users-subscribe@hudi.apache.org",children:"Hudi mailing list"})," - (send any msg to subscribe)"]}),"\n"]}),"\n",(0,o.jsx)(n.h3,{id:"come-to-office-hours-for-help",children:"Come to Office Hours for help"}),"\n",(0,o.jsxs)(n.p,{children:["Weekly office hours are ",(0,o.jsx)(n.a,{href:"/community/office_hours",children:"posted here"})]}),"\n",(0,o.jsx)(n.h3,{id:"community-calls",children:"Community Calls"}),"\n",(0,o.jsxs)(n.p,{children:["Attend ",(0,o.jsx)(n.a,{href:"/community/syncs#monthly-community-call",children:"monthly community calls"})," to learn best practices and see what others are building."]}),"\n",(0,o.jsx)(n.h2,{id:"contribute",children:"Contribute"}),"\n",(0,o.jsxs)(n.p,{children:["Apache Hudi welcomes you to join in on the fun and make a lasting impact on the industry as a whole. See our\n",(0,o.jsx)(n.a,{href:"/contribute/how-to-contribute",children:"contributor guide"})," to learn more, and don\u2019t hesitate to directly reach out to any of the\ncurrent committers to learn more."]}),"\n",(0,o.jsxs)(n.p,{children:["Have an idea, an ask, or feedback about a pain-point, but don\u2019t have time to contribute? Join the ",(0,o.jsx)(r.A,{title:"Hudi Slack Channel"}),"\nand share!"]})]})}function u(e={}){const{wrapper:n}={...(0,s.R)(),...e.components};return n?(0,o.jsx)(n,{...e,children:(0,o.jsx)(h,{...e})}):h(e)}},82915:(e,n,i)=>{i.d(n,{A:()=>s});var t=i(44586),o=i(74848);const s=e=>{let{title:n,isItalic:i}=e;const{siteConfig:s}=(0,t.A)(),{slackUrl:r}=s.customFields;return(0,o.jsx)("a",{href:r,style:{fontStyle:i?"italic":"normal"},target:"_blank",rel:"noopener noreferrer",children:n})}},28453:(e,n,i)=>{i.d(n,{R:()=>r,x:()=>a});var t=i(96540);const o={},s=t.createContext(o);function r(e){const n=t.useContext(s);return t.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function a(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(o):e.components||o:r(e.components),t.createElement(s.Provider,{value:n},e.children)}}}]);