import Vue from 'vue'
import App from './App.vue'
import './plugins/element.js'
import VueRouter from 'vue-router'
import Demo1 from './Demo.vue'
import Demo2 from './App2.vue'


Vue.use(VueRouter)
Vue.config.productionTip = false

const routes = [
    { path: '/demo1', component: Demo1},
    { path: '/demo2', component: Demo2 }
]

// 3. 创建 router 实例，然后传 `routes` 配置
// 你还可以传别的配置参数, 不过先这么简单着吧。
const router = new VueRouter({
    routes // (缩写) 相当于 routes: routes
})


new Vue({
  render: h => h(App),
    router,
}).$mount('#app')
