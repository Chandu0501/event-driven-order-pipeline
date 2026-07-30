import http from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    create_orders: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 25),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30s",
      preAllocatedVUs: Number(__ENV.VUS || 20),
      maxVUs: Number(__ENV.MAX_VUS || 100),
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"],
  },
};

const baseUrl = __ENV.BASE_URL || "http://localhost:8080";

export default function () {
  const suffix = `${__VU}-${__ITER}-${Date.now()}`;
  const response = http.post(
    `${baseUrl}/api/v1/orders`,
    JSON.stringify({
      customerId: `benchmark-customer-${__VU}`,
      productId: `product-${suffix}`,
      quantity: 1,
      totalAmount: 49.99,
    }),
    { headers: { "Content-Type": "application/json" } },
  );

  check(response, {
    "order accepted": (result) => result.status === 201,
  });
}
