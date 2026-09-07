const { test, expect } = require("@playwright/test");
const { startDetentRuntime } = require("./detent-runtime");

test.describe.configure({ mode: "serial" });

let runtime;

test.beforeAll(async () => {
  runtime = await startDetentRuntime("local-time", ["--demo", "screenshots"]);
});

test.afterAll(async () => {
  await runtime?.stop();
});

for (const updates of [false, true]) {
  test(`relative labels advance ${updates ? "through SSE morphs" : "on idle pages"}`, async ({ page }) => {
    await page.clock.install({ time: new Date("2026-07-10T18:00:00Z") });
    await page.clock.pauseAt(new Date("2026-07-10T18:00:00Z"));
    await page.addInitScript(() => {
      window.localTimeSources = [];
      window.EventSource = class extends EventTarget {
        constructor(url) {
          super();
          this.url = new URL(url, location.href).href;
          this.readyState = 1;
          window.localTimeSources.push(this);
          queueMicrotask(() => this.dispatchEvent(new Event("open")));
        }
        close() { this.readyState = 2; }
      };
      window.localTimeRegistrations = { intervals: 0, listeners: 0 };
      const setInterval = window.setInterval;
      window.setInterval = (...args) => {
        window.localTimeRegistrations.intervals++;
        return setInterval(...args);
      };
      const addEventListener = document.addEventListener.bind(document);
      document.addEventListener = (...args) => {
        window.localTimeRegistrations.listeners++;
        return addEventListener(...args);
      };
    });
    await page.goto(runtime.url, { waitUntil: "domcontentloaded" });
    const clock = page.locator("#live-clock time");
    await expect(clock).toBeVisible();
    const timestamp = await clock.getAttribute("datetime");
    await page.clock.setSystemTime(new Date(Date.parse(timestamp) + 37_000));
    await clock.evaluate((element) => window.__detentLocalTimeUpgrade(element));
    const initial = await clock.textContent();
    expect(initial).toContain("(37 sec. ago)");
    const absolute = initial.split(" (")[0];
    const title = await clock.getAttribute("title");
    const originalClock = await clock.elementHandle();
    const originalSnapshot = await page.locator("#snapshot").elementHandle();
    expect(await clock.evaluate((element) => element.closest("#snapshot") === null)).toBe(true);

    const registrations = await page.evaluate(() => {
      const script = Array.from(document.scripts).find((element) =>
        element.textContent.includes("var tokenPattern ="));
      window.localTimeScript = script.textContent;
      const incomingTime = document.querySelector("#live-clock time").cloneNode(true);
      incomingTime.id = "incoming-relative-time";
      window.localTimeSnapshot = document.getElementById("snapshot").innerHTML + incomingTime.outerHTML;
      window.localTimeSettles = 0;
      document.getElementById("snapshot").addEventListener("htmx:afterSettle", () => window.localTimeSettles++);
      return { ...window.localTimeRegistrations };
    });
    for (const step of [
      { elapsed: 1_000, relative: "38 sec. ago" },
      { elapsed: 51_000, relative: "89 sec. ago" },
      { elapsed: 1_000, relative: "1 min. ago" },
      { elapsed: 31_000, relative: "2 min. ago" },
    ]) {
      if (updates) {
        const settled = await page.evaluate(() => {
          const script = document.createElement("script");
          script.textContent = window.localTimeScript;
          document.head.append(script);
          script.remove();
          const count = window.localTimeSettles;
          window.localTimeSources[0].dispatchEvent(new MessageEvent("snapshot", {
            data: window.localTimeSnapshot,
          }));
          return count;
        });
        await page.clock.runFor(step.elapsed);
        await expect.poll(() => page.evaluate(() => window.localTimeSettles)).toBeGreaterThan(settled);
      } else {
        await page.clock.runFor(step.elapsed);
      }
      await expect(clock).toHaveText(`${absolute} (${step.relative})`);
      await expect(clock).toHaveAttribute("datetime", timestamp);
      await expect(clock).toHaveAttribute("title", title);
      if (updates) {
        await expect(page.locator("#incoming-relative-time")).toHaveText(`${absolute} (${step.relative})`);
      }
      expect(await originalClock.evaluate((element) => element.isConnected)).toBe(true);
      expect(await originalSnapshot.evaluate((element) => element.isConnected)).toBe(true);
    }
    expect(await page.evaluate(() => window.localTimeRegistrations)).toEqual(registrations);
    await expect(page.locator("#snapshot")).toHaveAttribute("hx-swap", "morph:innerHTML");
    await page.evaluate(() => {
      window.localTimeMutations = [];
      const observer = new MutationObserver((records) => window.localTimeMutations.push(...records.map((record) => record.type)));
      observer.observe(document.querySelector("#live-clock time"), { attributes: true, childList: true, characterData: true, subtree: true });
      document.querySelectorAll('time[data-local-time]:not([data-local-time-relative="true"])').forEach((element) => {
        observer.observe(element, { attributes: true, childList: true, characterData: true, subtree: true });
      });
    });
    await page.clock.runFor(1_000);
    expect(await page.evaluate(() => window.localTimeMutations)).toEqual([]);
  });
}

test("capacity notices render once in each browser timezone after morphs", async ({
  browser,
}) => {
  const cases = [
    { timezoneId: "America/Chicago", expected: "12:44 PM", morphed: "1:44 PM" },
    { timezoneId: "America/Los_Angeles", expected: "10:44 AM", morphed: "11:44 AM" },
  ];

  for (const testCase of cases) {
    const context = await browser.newContext({
      locale: "en-US",
      timezoneId: testCase.timezoneId,
    });
    const page = await context.newPage();
    await page.setExtraHTTPHeaders({
      "X-Detent-Demo-Scenario": "backend-capacity-outage",
    });
    await page.goto(`${runtime.url}/health/ui`, { waitUntil: "domcontentloaded" });

    const banner = page.locator("#backend-capacity-outage");
    await expect(banner).toBeVisible();
    await expect(banner.getByText("Backend openai at usage limit")).toHaveCount(1);
    const resetTime = banner.locator("time[data-local-time]");
    await expect(resetTime).toHaveCount(1);
    await expect(resetTime).toContainText(testCase.expected);
    await expect(resetTime).not.toContainText("UTC");
    await expect(resetTime).toHaveAttribute("title", "2026-07-10T17:44:00.000Z");

    await resetTime.evaluate((element) => {
      element.setAttribute("datetime", "2026-07-10T18:44:00Z");
      element.textContent = "…";
      element.dispatchEvent(new CustomEvent("htmx:afterSettle", { bubbles: true }));
    });
    await expect(resetTime).toContainText(testCase.morphed);

    await page.setExtraHTTPHeaders({
      "X-Detent-Demo-Scenario": "diagnostics-rate-limit-pressure",
    });
    await page.goto(`${runtime.url}/projects/dogfood/diagnostics`, {
      waitUntil: "domcontentloaded",
    });
    await page.locator("#snapshot").waitFor({ state: "visible" });
    await expect(page.locator("#snapshot time[data-local-time]").first()).toBeVisible();
    await expect(page.locator("#snapshot")).not.toContainText("{{detent-time:");
    await expect(page.locator("#snapshot")).not.toContainText("UTC");
    await context.close();
  }
});
